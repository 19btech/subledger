package com.fyntrac.common.repository;

import com.fyntrac.common.entity.Event;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

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

    // Count events by postingDate
    long countByPostingDate(Integer postingDate);

    // Delete events by instrumentId
    void deleteByInstrumentId(String instrumentId);

    // Delete events by postingDate
    void deleteByPostingDate(Integer postingDate);

    // Check if event exists by instrumentId and postingDate and eventId
    boolean existsByInstrumentIdAndPostingDateAndEventId(String instrumentId, Integer postingDate, String eventId);

    // Fixed: Removed status field from query since it doesn't exist in Event class
    @Query("{ 'postingDate': ?0 }")
    Page<Event> findAllByPostingDate(Integer postingDate, Pageable pageable);

    // Alternative query if you need filtering
    @Query("{ 'postingDate': ?0, 'instrumentId': ?1 }")
    Page<Event> findByPostingDateAndInstrumentId(Integer postingDate, String instrumentId, Pageable pageable);

    @Query(value = "{ 'postingDate': ?0, 'status': 'NOT_STARTED' }",
            fields = "{ 'instrumentId': 1 }")
    Page<Event> findInstrumentIdsByPostingDateAndStatusNotStarted(
            Integer postingDate, Pageable pageable);

    List<Integer> findDistinctPostingDateByPostingDateNotNull();

    @Query("{ 'postingDate': ?0, 'instrumentId': ?1 }")
    List<Event> findByPostingDateAndInstrumentId(Integer postingDate, String instrumentId);

}