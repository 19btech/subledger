package com.reserv.dataloader.service;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.ActivityDataValidationLog;
import com.fyntrac.common.enums.ValidationType;
import com.fyntrac.common.repository.ActivityDataValidationLogRepository;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Centralised logging service for activity-level validation errors.
 *
 * <p>Replaces direct {@code RefDataValidationLogRepository.saveAll()} calls inside
 * {@code TransactionActivityItemProcessor}, {@code InstrumentAttributeItemProcessor},
 * and {@code DynamicDataProcessor}.  Writes to the separate
 * {@code activity_data_validation_log} collection.
 *
 * <p>All writes are tenant-scoped via {@link TenantContextHolder}.
 */
@Service
public class ActivityValidationLogService {

    private static final Logger log = LoggerFactory.getLogger(ActivityValidationLogService.class);

    private final ActivityDataValidationLogRepository repository;

    public ActivityValidationLogService(ActivityDataValidationLogRepository repository) {
        this.repository = repository;
    }

    // ------------------------------------------------------------------
    // Primary API used by ItemProcessors
    // ------------------------------------------------------------------

    /**
     * Persists a list of validation errors for a single failing CSV row.
     *
     * @param errors         Errors produced by the validator
     * @param validationType Pipeline that produced the errors
     * @param sourceTable    Name of the entity / table being loaded
     * @param jobId          Current batch job execution id
     * @param tenantId       Tenant — used to scope the MongoDB write
     * @param ctx            Row-level contextual data (instrumentId, attributeId, dates, rowNumber)
     */
    public void saveAll(List<ItemValidationException.ValidationError> errors,
                        ValidationType validationType,
                        String sourceTable,
                        Long jobId,
                        String tenantId,
                        RowContext ctx) {

        if (errors == null || errors.isEmpty()) return;

        List<ActivityDataValidationLog> logs = errors.stream()
                .map(err -> ActivityDataValidationLog.builder()
                        .validationType(validationType)
                        .errorCode(err.getErrorCode())
                        .errorMessage(err.getMessage())
                        .fieldName(err.getColumn())
                        .rejectedValue(err.getValue())
                        .rowNumber(ctx.rowNumber())
                        .instrumentId(ctx.instrumentId())
                        .attributeId(ctx.attributeId())
                        .postingDate(ctx.postingDate())
                        .effectiveDate(ctx.effectiveDate())
                        .jobId(jobId)
                        .build())
                .collect(Collectors.toList());

        if (tenantId != null) {
            TenantContextHolder.runWithTenant(tenantId, () -> repository.saveAll(logs));
        } else {
            repository.saveAll(logs);
        }
        log.warn("Saved {} ActivityDataValidationLog entries (type={} table={} job={}).",
                logs.size(), validationType, sourceTable, jobId);
    }

    // ------------------------------------------------------------------
    // Row context — lightweight value object passed by each processor
    // ------------------------------------------------------------------

    /**
     * Contextual data extracted from the failing CSV row.
     *
     * <p>All fields are optional (null is fine). Processors fill what they know
     * before calling {@link #saveAll}.
     *
     * @param rowNumber    1-based row counter maintained by the processor
     * @param instrumentId raw instrumentId value (may be null / empty)
     * @param attributeId  raw attributeId value (may be null / empty)
     * @param postingDate  YYYYMMDD integer (0 when not parsed yet)
     * @param effectiveDate YYYYMMDD integer (0 when not parsed yet)
     */
    public record RowContext(
            Long rowNumber,
            String instrumentId,
            String attributeId,
            Integer postingDate,
            Integer effectiveDate) {

        /** Convenience factory when no context is available. */
        public static RowContext empty() {
            return new RowContext(null, null, null, null, null);
        }

        /** Convert 0 (sentinel) to null so the DB field is omitted. */
        public Integer postingDate() {
            return (postingDate != null && postingDate == 0) ? null : postingDate;
        }

        public Integer effectiveDate() {
            return (effectiveDate != null && effectiveDate == 0) ? null : effectiveDate;
        }
    }
}
