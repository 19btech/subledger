package com.fyntrac.common.repository;

import com.fyntrac.common.entity.AttributeLevelLtd;
import com.fyntrac.common.entity.TransactionActivity;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface AttributeLevelBalanceRepository extends MongoRepository<AttributeLevelLtd, String> {

    @Query("{ 'instrumentId': ?0, 'attributeId': ?1, 'postingDate': ?2, '\n" +
            "metricName': { $in: ?3 } }")
    List<AttributeLevelLtd> findBalanceByMetrics(String instrumentId, String attributeId,
                                                       Integer postingDate , List<String>
                                                         metricNames);

    // For multiple metrics - get latest for each metric as of postingDate
    @Aggregation(pipeline = {
            "{ $match: { " +
                    "  'instrumentId': ?0, " +
                    "  'attributeId': ?1, " +
                    "  'postingDate': { $lte: ?2 }, " +
                    "  'metricName': { $in: ?3 } " +
                    "} }",
            "{ $sort: { 'postingDate': -1 } }",
            "{ $group: { " +
                    "  '_id': '$metricName', " +
                    "  'document': { $first: '$$ROOT' } " +
                    "} }",
            "{ $replaceRoot: { newRoot: '$document' } }"
    })
    List<AttributeLevelLtd> findLatestBalanceByMetrics(String instrumentId, String attributeId,
                                                       Integer postingDate, List<String> metricNames);

    @Query("{ 'instrumentId': ?0, 'attributeId': ?1, 'postingDate': ?2 'metricName': ?3 }")
    Optional<AttributeLevelLtd> findActiveByMetricName(String instrumentId, String attributeId,
                                                              Integer postingDate, String metricName);

    @Aggregation(pipeline = {
            "{ $match: { " +
                    "  'instrumentId': ?0, " +
                    "  'attributeId': ?1, " +
                    "  'postingDate': { $lte: ?2 }, " +
                    "  'metricName': ?3, " +
                    "  'isActive': true " +
                    "} }",
            "{ $sort: { 'postingDate': -1 } }",
            "{ $limit: 1 }"
    })
    Optional<AttributeLevelLtd> findLatestByPostingDate(String instrumentId, String attributeId,
                                                        Integer postingDate, String metricName);
}
