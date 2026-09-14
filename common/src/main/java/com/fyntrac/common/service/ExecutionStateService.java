package com.fyntrac.common.service;

import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.repository.ExecutionStateRepository;
import com.fyntrac.common.repository.MemcachedRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class ExecutionStateService extends CacheBasedService<ExecutionState> {

    private final String key;
    private final ExecutionStateRepository executionStateRepository;

    public ExecutionStateService(ExecutionStateRepository executionStateRepository, DataService<ExecutionState> dataService, MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
        this.executionStateRepository = executionStateRepository;
        this.key = String.format("%s-%s", this.getDataService().getTenantId(), "EXECUTION-STATE");
    }

    @Override
    public ExecutionState save(ExecutionState state) {
        return executionStateRepository.save(state);
    }

    /**
     * Versions the ExecutionState:
     * 1. Closes EVERY currently-open record (sets endDate = now on all docs with endDate == null),
     *    not just the one referenced by state.getId(). This is deliberately belt-and-suspenders:
     *    a raw save() elsewhere (bypassing this method) or a race between two concurrent job
     *    completions can otherwise leave more than one "open" record behind, and
     *    getExecutionState()/fetchLatest() would then have no reliable way to tell which is
     *    truly the latest.
     * 2. Inserts a brand new document (id = null) with the new values and endDate = null.
     *
     * This is triggered at every point in the codebase where setLastExecutionDate is called,
     * meaning a state transition has occurred (old executionDate → lastExecutionDate, new date set).
     */
    public ExecutionState update(ExecutionState state) {
        // 1. Close every currently-open record, not just state's own id
        Query closeQuery = new Query(Criteria.where("endDate").isNull());
        Update closeUpdate = new Update().set("endDate", new Date());
        this.dataService.update(closeQuery, closeUpdate, ExecutionState.class);

        // 2. Insert a fresh document — null id forces a MongoDB insert
        state.setId(null);
        state.setEndDate(null);
        ExecutionState saved = this.dataService.save(state);

        // 3. Cache the new active state
        this.memcachedRepository.putInCache(key, saved);
        return saved;
    }

    /**
     * Returns the currently active ExecutionState.
     * <p>
     * "Active" is nominally "the one with endDate == null", but that isn't guaranteed to be a
     * single document — a stray raw save() or a completion-listener race can leave more than one
     * open record behind. To stay correct even then, this always picks the open record with the
     * highest executionDate, so a UI showing "latest loaded posting date" can never regress to an
     * older value just because an older record was never closed out.
     * Falls back to a zeroed-out default if no active record exists.
     */
    public ExecutionState getExecutionState() {
        Query query = new Query(Criteria.where("endDate").isNull())
                .with(org.springframework.data.domain.Sort.by(
                        org.springframework.data.domain.Sort.Direction.DESC, "executionDate"))
                .limit(1);
        ExecutionState active = this.dataService.findOne(query, ExecutionState.class);

        if (active != null) {
            this.memcachedRepository.putInCache(this.key, active);
            return active;
        }

        // No active state yet — return a safe default (not persisted)
        ExecutionState defaultState = ExecutionState.builder()
                .executionDate(0)
                .lastExecutionDate(0)
                .build();
        this.memcachedRepository.putInCache(this.key, defaultState);
        return defaultState;
    }

    /**
     * Returns all execution state records (active + historical).
     */
    @Override
    public Collection<ExecutionState> fetchAll() {
        return this.dataService.fetchAllData(ExecutionState.class);
    }

    /**
     * Returns the currently active (latest) ExecutionState — see {@link #getExecutionState()} for
     * why "latest" is resolved by highest executionDate rather than trusting there is only one
     * open record. Throws if no active record exists.
     */
    public ExecutionState fetchLatest() {
        Query query = new Query(Criteria.where("endDate").isNull())
                .with(org.springframework.data.domain.Sort.by(
                        org.springframework.data.domain.Sort.Direction.DESC, "executionDate"))
                .limit(1);
        ExecutionState active = this.dataService.findOne(query, ExecutionState.class);
        if (active == null) {
            throw new NoSuchElementException("No active ExecutionState found");
        }
        return active;
    }

    /**
     * Rollback ExecutionState history to just before the given postingDate.
     *
     * Called during cleanup when re-executing an earlier posting date
     * (i.e., postingDate < current executionDate). Steps:
     *
     * 1. Delete all ExecutionState records where executionDate >= postingDate
     *    (these represent execution runs that are being invalidated).
     * 2. Find the latest surviving record (highest executionDate < postingDate)
     *    which may be closed (endDate != null) and re-open it (set endDate = null).
     *
     * After this call, getExecutionState() will return the re-opened record.
     */
    public void rollbackToBeforeDate(int postingDate) {
        // 1. Delete all records >= postingDate (both open and historical)
        Query deleteQuery = new Query(Criteria.where("executionDate").gte(postingDate));
        this.dataService.deleteByQuery(deleteQuery, ExecutionState.class);

        // 2. Find the latest surviving record (highest executionDate < postingDate)
        Query latestQuery = new Query(Criteria.where("executionDate").lt(postingDate))
                .with(org.springframework.data.domain.Sort.by(
                        org.springframework.data.domain.Sort.Direction.DESC, "executionDate"))
                .limit(1);
        ExecutionState latest = this.dataService.findOne(latestQuery, ExecutionState.class);

        if (latest != null) {
            // Re-open it — clear endDate so it becomes the active record again
            Query reopenQuery = new Query(Criteria.where("_id").is(latest.getId()));
            Update reopenUpdate = new Update().unset("endDate");
            this.dataService.updateFirst(reopenQuery, reopenUpdate, ExecutionState.class);

            // Refresh cache
            latest.setEndDate(null);
            this.memcachedRepository.putInCache(this.key, latest);
        } else {
            // No prior state exists — cache a zeroed default
            ExecutionState defaultState = ExecutionState.builder()
                    .executionDate(0)
                    .lastExecutionDate(0)
                    .build();
            this.memcachedRepository.putInCache(this.key, defaultState);
        }
    }

    @Override
    public void loadIntoCache() throws ExecutionException, InterruptedException {
        // no-op
    }

    /**
     * Retrieves the single execution state matching a specific business posting date.
     *
     * @param executionDate The YYYYMMDD formatted integer sequence.
     * @return The active ExecutionState document metadata.
     */
    public ExecutionState getExecutionStateByDate(Integer executionDate) {
        log.debug("Fetching execution state profile for date context: [{}]", executionDate);
        return executionStateRepository.findByExecutionDate(executionDate)
                .orElseThrow(() -> new NoSuchElementException(
                        "No execution tracking instance found for the specified date: " + executionDate));
    }

    /**
     * Retrieves the execution state associated with a previous system runtime date milestone.
     *
     * @param lastExecutionDate The historical YYYYMMDD formatted integer boundary.
     * @return The matching ExecutionState record.
     */
    public ExecutionState getExecutionStateByLastDate(Integer lastExecutionDate) {
        log.debug("Fetching historical execution profile matching prior checkpoint: [{}]", lastExecutionDate);
        return executionStateRepository.findByLastExecutionDate(lastExecutionDate)
                .orElseThrow(() -> new NoSuchElementException(
                        "No execution tracking instance found matching prior checkpoint date: " + lastExecutionDate));
    }

    /**
     * Custom JSON query abstraction execution route bypassing standard derived query evaluation pipelines.
     * Useful for performance critical isolation checks or targeted validation routines.
     *
     * @param executionDate The target date configuration.
     * @return The specific tracking state entity document.
     */
    public ExecutionState getCustomExecutionStateByDate(Integer executionDate) {
        log.debug("Executing optimized custom query mapping for execution target: [{}]", executionDate);
        return executionStateRepository.findCustomByExecutionDate(executionDate)
                .orElseThrow(() -> new NoSuchElementException(
                        "Custom query returned zero matches for execution date constraint: " + executionDate));
    }

    /**
     * Deletes a single execution state document matching an exact date configuration.
     * Typically used for resetting a specific day's batch run configuration.
     *
     * @param postingDate The target YYYYMMDD formatted integer.
     */
    public void deleteStateByExactDate(Integer postingDate) {
        log.warn("Initiating targeted deletion of execution state metadata for date: [{}]", postingDate);
        executionStateRepository.deleteByExecutionDate(postingDate);
        log.info("Successfully dropped execution state record for date: [{}]", postingDate);
    }

    /**
     * Purges the target date AND all future processing timelines from the system (Date >= target).
     * This is used to roll back the system state during historical reprocessing routines.
     *
     * @param postingDate The starting boundary date integer (inclusive).
     */
    public void purgeStateFromDateOnwards(Integer postingDate) {
        log.warn("CRITICAL: Executing rolling purge of all future execution states starting from date (INCLUSIVE): [{}]", postingDate);
        executionStateRepository.deleteByExecutionDateGreaterThanEqual(postingDate);
        log.info("Rolling state purge completed successfully for timelines equal to or following: [{}]", postingDate);
    }

    /**
     * Purges only strict future processing timelines, preserving the passed date intact (Date > target).
     * Useful when you want to clear out accidental trailing runs but keep the current day's active state safe.
     *
     * @param postingDate The anchor date integer (exclusive boundary).
     */
    public void purgeStateStrictlyAfterDate(Integer postingDate) {
        log.warn("Executing data cleanup for all trailing execution states following date (EXCLUSIVE): [{}]", postingDate);
        executionStateRepository.deleteByExecutionDateGreaterThan(postingDate);
        log.info("Strictly post-date state cleanups finalized for timelines ahead of: [{}]", postingDate);
    }
}
