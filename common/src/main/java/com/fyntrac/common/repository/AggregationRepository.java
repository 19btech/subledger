package com.fyntrac.common.repository;

import com.fyntrac.common.entity.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface AggregationRepository extends MongoRepository<Aggregation, String> {

    Optional<Aggregation> findByMetricName(String metricName);

    boolean existsByMetricName(String metricName);

    /**
     * Fetches all active (non soft-deleted) aggregation entries. Records missing the isDeleted
     * field entirely (pre-existing data) are treated as active.
     */
    @Query("{ '$or': [ { 'isDeleted': false }, { 'isDeleted': { '$exists': false } } ] }")
    List<Aggregation> findByIsDeletedFalse();

    void deleteById(String id);
    void delete(Aggregation entity);
    void deleteAll();
}
