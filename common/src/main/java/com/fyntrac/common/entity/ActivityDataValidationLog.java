package com.fyntrac.common.entity;

import com.fyntrac.common.enums.ValidationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

/**
 * Stores per-row validation errors from activity-level batch pipelines.
 *
 * <p>Distinct from {@link RefDataValidationLog} (reference-data errors) — this collection
 * captures errors from transactional / activity CSV imports and carries the extra
 * contextual fields needed for root-cause triage (instrumentId, attributeId, dates, rowNumber).
 *
 * <p>Collection: {@code activity_data_validation_log}
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "activity_data_validation_log")
// Covers ActivityDataValidationLogRepository's actual hot queries (findByJobId,
// findByJobIdAndInstrumentId, findByJobIdAndErrorCode, findByJobIdAndValidationType — all as
// index prefixes off jobId, which had no index at all before this).
@CompoundIndex(def = "{'jobId': 1, 'instrumentId': 1}", name = "ActivityDataValidationLog_job_instrument_index")
// Also covers the instrumentId+attributeId+postingDate combination directly, for any future
// per-instrument/date triage query outside the jobId-scoped ones above.
@CompoundIndex(def = "{'instrumentId': 1, 'attributeId': 1, 'postingDate': 1}", name = "ActivityDataValidationLog_instrument_attribute_postingdate_index")
public class ActivityDataValidationLog {

    /** Auto-generated MongoDB _id. */
    @Id
    private String id;

    /**
     * Pipeline that raised the error.
     * ACTIVITY   → TransactionActivity / InstrumentAttribute
     * CUSTOM_ACTIVITY → DynamicTable
     */
    private ValidationType validationType;

    /** Standardised error code (e.g. ERR_REQ_02, ERR_REF_03). */
    private String errorCode;

    /** Human-readable error description. */
    private String errorMessage;

    /** CSV column / field name that failed validation. */
    private String fieldName;

    /** Raw value read from the CSV that triggered the error. */
    private String rejectedValue;

    /** 1-based sequence number of the failing row in the source file. */
    private Long rowNumber;

    /** InstrumentId from the failing row (may be null when the field itself is missing). */
    private String instrumentId;

    /** AttributeId from the failing row (may be null when the field itself is missing). */
    private String attributeId;

    /**
     * Posting date from the failing row in YYYYMMDD integer form
     * (consistent with how it is stored in TransactionActivity / InstrumentAttribute).
     */
    private Integer postingDate;

    /**
     * Effective date from the failing row in YYYYMMDD integer form.
     */
    private Integer effectiveDate;

    /** Batch job execution id that triggered this log entry. */
    private Long jobId;

    /** Audit timestamp — defaults to now when first accessed. */
    private LocalDateTime createdAt;

    public LocalDateTime getCreatedAt() {
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
        return createdAt;
    }
}
