package com.fyntrac.common.repository;

import com.fyntrac.common.entity.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AggregationRepository extends MongoRepository<Aggregation, String> {

    Optional<Aggregation> findByMetricName(String metricName);

    boolean existsByMetricName(String metricName);
}
