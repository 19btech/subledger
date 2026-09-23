package com.fyntrac.common.repository;

import com.fyntrac.common.entity.Event;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public interface EventRepository extends MongoRepository<Event, String> {

    // Find events by postingDate
    Page<Event> findByPostingDate(Integer postingDate, Pageable pageable);

    // Find events by instrumentId
    List<Event> findByInstrumentId(String instrumentId);

    // Find events by instrumentId with pagination
    Page<Event> findByInstrumentId(String instrumentId, Pageable pageable);

    // Find events by eventId
    List<Event> findByEventId(String eventId);

    // Find events by postingDate and eventId
    List<Event> findByPostingDateAndEventId(Integer postingDate, String eventId);

    // Find events by effectiveDate range
    @Query("{ 'effectiveDate': { $gte: ?0, $lte: ?1 } }")
    List<Event> findByEffectiveDateBetween(Integer startDate, Integer endDate);

    // Find events by postingDate range
    @Query("{ 'postingDate': { $gte: ?0, $lte: ?1 } }")
    List<Event> findByPostingDateBetween(Integer startDate, Integer endDate);

    // Find events by instrumentId and postingDate range
    @Query("{ 'instrumentId': ?0, 'postingDate': { $gte: ?1, $lte: ?2 } }")
    List<Event> findByInstrumentIdAndPostingDateBetween(String instrumentId, Integer startDate, Integer endDate);

    // Find events by multiple instrumentIds
    @Query("{ 'instrumentId': { $in: ?0 } }")
    List<Event> findByInstrumentIdIn(List<String> instrumentIds);

    // Find events by priority
    List<Event> findByPriority(Integer priority);

    // Find events by priority range
    @Query("{ 'priority': { $gte: ?0, $lte: ?1 } }")
    List<Event> findByPriorityBetween(Integer minPriority, Integer maxPriority);

    // Count events by instrumentId
    long countByInstrumentId(String instrumentId);

    // Count events by postingDate (raw event rows — use countDistinctInstrumentsByPostingDate for instrument count)
    long countByPostingDate(Integer postingDate);

    /**
     * Count of DISTINCT instrumentIds for a given postingDate.
     * This is the correct denominator for progress tracking:
     *   - execution batches instruments (not raw events)
     *   - one instrument may have multiple event rows
     *
     * Pipeline: $match postingDate → $group by instrumentId → $count
     * Excludes the shared reference-table events (Event.SHARED_REFERENCE_INSTRUMENT_ID) — not an instrument.
     */
    @Aggregation(pipeline = {
        "{ '$match': { 'postingDate': ?0, 'instrumentId': { '$ne': '__SHARED_REFERENCE__' } } }",
        "{ '$group': { '_id': '$instrumentId' } }",
        "{ '$count': 'total' }"
    })
    Long countDistinctInstrumentsByPostingDate(Integer postingDate);

    // Delete events by instrumentId
    @Transactional
    void deleteByInstrumentId(String instrumentId);

    // Delete events by postingDate
    @Transactional
    void deleteByPostingDate(Integer postingDate);

    @Transactional
    void deleteByPostingDateGreaterThanEqual(Integer postingDate);

    // Check if event exists by instrumentId and postingDate and eventId
    boolean existsByInstrumentIdAndPostingDateAndEventId(String instrumentId, Integer postingDate, String eventId);

    // Fixed: Removed status field from query since it doesn't exist in Event class
    @Query("{ 'postingDate': ?0 }")
    Page<Event> findAllByPostingDate(Integer postingDate, Pageable pageable);

    // Alternative query if you need filtering
    @Query("{ 'postingDate': ?0, 'instrumentId': ?1 }")
    Page<Event> findByPostingDateAndInstrumentId(Integer postingDate, String instrumentId, Pageable pageable);

    // Excludes the shared reference-table events (Event.SHARED_REFERENCE_INSTRUMENT_ID) — not an instrument.
    @Query(value = "{ 'postingDate': ?0, 'status': 'NOT_STARTED', 'instrumentId': { '$ne': '__SHARED_REFERENCE__' } }",
            fields = "{ 'instrumentId': 1 }")
    Page<Event> findInstrumentIdsByPostingDateAndStatusNotStarted(
            Integer postingDate, Pageable pageable);

    List<Integer> findDistinctPostingDateByPostingDateNotNull();

    @Query("{ 'postingDate': ?0, 'instrumentId': ?1 }")
    List<Event> findByPostingDateAndInstrumentId(Integer postingDate, String instrumentId);

    /**
     * The shared reference-table events for a posting date — written once per run instead of once
     * per instrument, see Event.SHARED_REFERENCE_INSTRUMENT_ID.
     */
    default List<Event> findSharedReferenceEvents(Integer postingDate) {
        return findByPostingDateAndInstrumentId(postingDate, Event.SHARED_REFERENCE_INSTRUMENT_ID);
    }

    /**
     * Every event an instrument's model execution sees: its own plus the shared reference-table
     * events. Use this, not findByPostingDateAndInstrumentId, wherever a model or diagnostic needs
     * an instrument's full event set. Shared events come first, matching the order the
     * per-instrument copies they replace were written in.
     */
    default List<Event> findEventsForInstrument(Integer postingDate, String instrumentId) {
        return withSharedReferenceEvents(findSharedReferenceEvents(postingDate),
                findByPostingDateAndInstrumentId(postingDate, instrumentId));
    }

    /** For loops over many instruments: fetch findSharedReferenceEvents once, then combine per instrument. */
    static List<Event> withSharedReferenceEvents(List<Event> sharedReferenceEvents, List<Event> instrumentEvents) {
        List<Event> events = new java.util.ArrayList<>(sharedReferenceEvents.size() + instrumentEvents.size());
        events.addAll(sharedReferenceEvents);
        events.addAll(instrumentEvents);
        return events;
    }

}