package com.fyntrac.common.repository;

import com.fyntrac.common.entity.Errors;
import com.fyntrac.common.enums.ErrorCategory;
import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.enums.ErrorType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public interface ErrorsRepository extends MongoRepository<Errors, String> {

    // ───────────────────────────────────────────────
    // Basic find operations
    // ───────────────────────────────────────────────
    Errors findByErrorId(String errorId);

    List<Errors> findByCode(ErrorCode code);
    Page<Errors> findByCode(ErrorCode code, Pageable pageable);

    List<Errors> findByMessageContainingIgnoreCase(String message);

    // ───────────────────────────────────────────────
    // Find by related entity identifiers
    // ───────────────────────────────────────────────
    List<Errors> findByInstrumentId(String instrumentId);
    Page<Errors> findByInstrumentId(String instrumentId, Pageable pageable);

    List<Errors> findByAttributeId(String attributeId);
    List<Errors> findByModelId(String modelId);

    List<Errors> findByInstrumentIdAndAttributeId(String instrumentId, String attributeId);

    // ───────────────────────────────────────────────
    // Find by job
    // ───────────────────────────────────────────────
    List<Errors> findByJobId(String jobId);
    Page<Errors> findByJobId(String jobId, Pageable pageable);

    long countByJobId(String jobId);

    // ───────────────────────────────────────────────
    // Find by source metadata
    // ───────────────────────────────────────────────
    List<Errors> findBySourceTable(String sourceTable);
    List<Errors> findBySourceColumn(String sourceColumn);
    List<Errors> findBySourceTableAndSourceColumn(String sourceTable, String sourceColumn);

    List<Errors> findByRowNum(Long rowNum);

    // ───────────────────────────────────────────────
    // Find by category / type / severity
    // ───────────────────────────────────────────────
    List<Errors> findByErrorCategory(ErrorCategory errorCategory);
    List<Errors> findByErrorType(ErrorType errorType);

    List<Errors> findByErrorCategoryAndErrorType(ErrorCategory errorCategory, ErrorType errorType);

    List<Errors> findByIsWarningTrue();
    List<Errors> findByIsWarningFalse();

    // ───────────────────────────────────────────────
    // Date-based queries
    // ───────────────────────────────────────────────
    List<Errors> findByExecutionDateAfter(Date date);
    List<Errors> findByExecutionDateBefore(Date date);

    @Query("{ 'executionDate': { $gte: ?0, $lte: ?1 } }")
    List<Errors> findByExecutionDateBetween(Date startDate, Date endDate);

    List<Errors> findByCreatedTimestampAfter(Date date);

    @Query("{ 'createdTimestamp': { $gte: ?0, $lte: ?1 } }")
    List<Errors> findByCreatedTimestampBetween(Date startDate, Date endDate);

    List<Errors> findByPostingDate(Date postingDate);
    Page<Errors> findByPostingDate(Date postingDate, Pageable pageable);

    // ───────────────────────────────────────────────
    // Complex @Query searches
    // ───────────────────────────────────────────────
    @Query("{ 'instrumentId': ?0, 'executionDate': { $gte: ?1, $lte: ?2 } }")
    List<Errors> findByInstrumentIdAndExecutionDateBetween(String instrumentId, Date startDate, Date endDate);

    @Query("{ 'modelId': ?0, 'executionDate': { $gte: ?1, $lte: ?2 } }")
    List<Errors> findByModelIdAndExecutionDateBetween(String modelId, Date startDate, Date endDate);

    @Query("{ 'jobId': ?0, 'errorType': ?1 }")
    List<Errors> findByJobIdAndErrorType(String jobId, ErrorType errorType);

    @Query("{ '$and': [ " +
            "{ 'instrumentId': ?0 }, " +
            "{ 'isWarning': ?1 }, " +
            "{ 'errorCategory': ?2 } " +
            "] }")
    List<Errors> findByInstrumentIdAndIsWarningAndErrorCategory(String instrumentId, boolean isWarning, ErrorCategory errorCategory);

    @Query("{ '$or': [ " +
            "{ 'message': { $regex: ?0, $options: 'i' } }, " +
            "{ 'stacktrace': { $regex: ?0, $options: 'i' } } " +
            "] }")
    List<Errors> searchByMessageOrStacktrace(String searchTerm);

    @Query("{ '$and': [ " +
            "{ 'instrumentId': ?0 }, " +
            "{ '$or': [ " +
            "  { 'message': { $regex: ?1, $options: 'i' } }, " +
            "  { 'stacktrace': { $regex: ?1, $options: 'i' } } " +
            "] } " +
            "] }")
    List<Errors> searchByInstrumentIdAndMessageOrStacktrace(String instrumentId, String searchTerm);

    // ───────────────────────────────────────────────
    // Count queries
    // ───────────────────────────────────────────────
    long countByCode(ErrorCode code);
    long countByInstrumentId(String instrumentId);
    long countByModelId(String modelId);
    long countByErrorCategory(ErrorCategory errorCategory);
    long countByErrorType(ErrorType errorType);
    long countByIsWarningTrue();
    long countByIsWarningFalse();
    long countBySourceTable(String sourceTable);

    // ───────────────────────────────────────────────
    // Aggregation: distinct counts
    // ───────────────────────────────────────────────
    @Aggregation(pipeline = {
            "{ '$match': { 'instrumentId': ?0 } }",
            "{ '$group': { '_id': '$code' } }",
            "{ '$count': 'total' }"
    })
    Long countDistinctErrorCodesByInstrumentId(String instrumentId);

    @Aggregation(pipeline = {
            "{ '$match': { 'jobId': ?0 } }",
            "{ '$group': { '_id': '$instrumentId' } }",
            "{ '$count': 'total' }"
    })
    Long countDistinctInstrumentsByJobId(String jobId);

    @Aggregation(pipeline = {
            "{ '$match': { 'modelId': ?0 } }",
            "{ '$group': { '_id': '$attributeId' } }",
            "{ '$count': 'total' }"
    })
    Long countDistinctAttributesByModelId(String modelId);

    // ───────────────────────────────────────────────
    // Delete operations
    // ───────────────────────────────────────────────
    void deleteByErrorId(String errorId);
    void deleteByInstrumentId(String instrumentId);
    void deleteByJobId(String jobId);
    void deleteByModelId(String modelId);
    void deleteByExecutionDateBefore(Date date);
    long deleteByPostingDate(Date postingDate);

    // ───────────────────────────────────────────────
    // Update operations
    // ───────────────────────────────────────────────
    @Query("{ '_id': ?0 }")
    @Update("{ '$set': { 'message': ?1, 'updatedTimestamp': ?2 } }")
    void updateMessage(String id, String message, Date updatedTimestamp);

    @Query("{ '_id': ?0 }")
    @Update("{ '$set': { 'isWarning': ?1 } }")
    void updateIsWarning(String id, boolean isWarning);

    @Query("{ 'jobId': ?0 }")
    @Update("{ '$set': { 'errorType': ?1 } }")
    void updateErrorTypeByJobId(String jobId, ErrorType errorType);

    // ───────────────────────────────────────────────
    // Existence checks
    // ───────────────────────────────────────────────
    boolean existsByErrorId(String errorId);
    boolean existsByInstrumentId(String instrumentId);
    boolean existsByJobId(String jobId);

    // ───────────────────────────────────────────────
    // Pagination with sorting helpers
    // ───────────────────────────────────────────────
    Page<Errors> findByCodeAndInstrumentId(ErrorCode code, String instrumentId, Pageable pageable);

    @Query("{ 'code': ?0, 'errorType': ?1 }")
    Page<Errors> findByCodeAndErrorType(ErrorCode code, ErrorType errorType, Pageable pageable);
}
