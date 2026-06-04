package com.fyntrac.common.repository;

import com.fyntrac.common.entity.ModelExecutionBatchLog;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public interface ModelExecutionBatchLogRepository extends MongoRepository<ModelExecutionBatchLog, String> {

    /** All batch logs for a given tenant + postingDate + logType (primary summary query). */
    List<ModelExecutionBatchLog> findByTenantIdAndPostingDateAndLogType(
            String tenantId, Integer postingDate, String logType);

    /**
     * COUNT-only variant — used by the progress endpoint.
     * Never loads document bodies; served entirely from the compound index.
     */
    long countByTenantIdAndPostingDateAndLogType(
            String tenantId, Integer postingDate, String logType);

    /** Count by status for lightweight success/fail tallying. */
    long countByTenantIdAndPostingDateAndLogTypeAndStatus(
            String tenantId, Integer postingDate, String logType, String status);

    /** Per-job drill-down (future use). */
    List<ModelExecutionBatchLog> findByTenantIdAndJobIdAndLogType(
            String tenantId, String jobId, String logType);

    /** Legacy / backwards-compat (no tenant filter). */
    List<ModelExecutionBatchLog> findByPostingDateAndLogType(Integer postingDate, String logType);

    @Transactional
    void deleteByPostingDate(Integer postingDate);

    @Transactional
    void deleteByPostingDateGreaterThanEqual(Integer postingDate);
}
