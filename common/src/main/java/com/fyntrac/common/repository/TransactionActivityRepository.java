package com.fyntrac.common.repository;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.enums.Source;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TransactionActivityRepository extends MongoRepository<TransactionActivity, String> {

    @Query("{ 'instrumentId': ?0, 'attributeId': ?1, 'postingDate': ?2 }")
    Optional<TransactionActivity> findActiveByPostingDate(String instrumentId, String attributeId, Integer postingDate);

    @Query("{ 'postingDate': ?0 }")
    Optional<TransactionActivity> findAllByPostingDate(Integer postingDate);

    @Query("{ 'instrumentId': ?0, 'attributeId': ?1, 'postingDate': ?2, 'transactionName': { $in: ?3 } }")
    List<TransactionActivity> findActiveByTransactions(String instrumentId, String attributeId,
                                                       Integer postingDate , List<String> transactions);

    @Query("{ 'instrumentId': ?0, 'attributeId': ?1, 'postingDate': ?2 'transactionName': ?3 }")
    Optional<TransactionActivity> findActiveByTransactionName(String instrumentId, String attributeId,
                                                              Integer postingDate, String transactionName);

    @Query("{ 'instrumentId': ?0, 'attributeId': ?1, 'source': ?2 }")
    Optional<TransactionActivity> findActiveBySource(String instrumentId, String attributeId, Source source);

    /**
     * Aggregate total amount grouped by transactionName and effectiveDate
     */
    @Aggregation(pipeline = {
            "{ $match: { " +
                    "instrumentId: ?0, " +
                    "attributeId: ?1, " +
                    "postingDate: ?2, " +
                    "transactionName: { $in: ?3 }, " +
                    "amount: { $exists: true, $ne: null } " +  // Ensure amount exists and is not null
                    "} }",
            "{ $group: { " +
                    "_id: { " +
                    "transactionName: '$transactionName', " +
                    "effectiveDate: '$effectiveDate' " +
                    "}, " +
                    "totalAmount: { $sum: { $ifNull: ['$amount', 0] } } " +  // Handle null amounts
                    "} }",
            "{ $project: { " +
                    "_id: 0, " +
                    "transactionName: '$_id.transactionName', " +
                    "effectiveDate: '$_id.effectiveDate', " +
                    "totalAmount: 1 " +
                    "} }"
    })
    List<Records.TransactionActivityAmountRecord> aggregateAmountByTransactionAndEffectiveDate(
            String instrumentId, String attributeId, Integer postingDate, List<String> transactions);
}
