package com.fyntrac.common.service;

import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.repository.MemcachedRepository;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

@Service
public class ExecutionStateService extends CacheBasedService<ExecutionState> {

    private final String key;

    public ExecutionStateService(DataService<ExecutionState> dataService, MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
        this.key = String.format("%s-%s", this.getDataService().getTenantId(), "EXECUTION-STATE");
    }

    @Override
    public ExecutionState save(ExecutionState state) {
        return null;
    }

    /**
     * Versions the ExecutionState:
     * 1. Closes the current active record (sets endDate = now) if it has an id.
     * 2. Inserts a brand new document (id = null) with the new values and endDate = null.
     *
     * This is triggered at every point in the codebase where setLastExecutionDate is called,
     * meaning a state transition has occurred (old executionDate → lastExecutionDate, new date set).
     */
    public ExecutionState update(ExecutionState state) {
        // 1. Close the existing active record
        if (state.getId() != null) {
            Query closeQuery = new Query(Criteria.where("_id").is(state.getId()));
            Update closeUpdate = new Update().set("endDate", new Date());
            this.dataService.updateFirst(closeQuery, closeUpdate, ExecutionState.class);
        }

        // 2. Insert a fresh document — null id forces a MongoDB insert
        state.setId(null);
        state.setEndDate(null);
        ExecutionState saved = this.dataService.save(state);

        // 3. Cache the new active state
        this.memcachedRepository.putInCache(key, saved);
        return saved;
    }

    /**
     * Returns the currently active ExecutionState (the one with endDate == null).
     * Falls back to a zeroed-out default if no active record exists.
     */
    public ExecutionState getExecutionState() {
        Query query = new Query(Criteria.where("endDate").isNull());
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
     * Returns the currently active (latest) ExecutionState.
     * Throws if no active record exists.
     */
    public ExecutionState fetchLatest() {
        Query query = new Query(Criteria.where("endDate").isNull());
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
}
