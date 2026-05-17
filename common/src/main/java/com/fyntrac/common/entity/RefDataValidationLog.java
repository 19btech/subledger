package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Entity to log validation errors encountered during reference data processing.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "RefDataValidationLog")
public class RefDataValidationLog {

    /**
     * Primary key, Auto-generated String ID for MongoDB.
     */
    @Id
    private String id;

    /**
     * Identifies the name of the source table being processed.
     */
    private String sourceTable;

    /**
     * Identifies the specific column yielding the validation error.
     */
    private String sourceColumn;

    /**
     * Identifies the actual value of the column yielding the validation error.
     */
    private String sourceColumnValue;

    /**
     * Represents record sequence number in the input.
     */
    private Long rowNum;

    /**
     * Categorizes error source: DATA or PROCESSING.
     */
    private String errorCategory;

    /**
     * Classification of error impact: ERROR or WARNING.
     */
    private String severity;

    /**
     * Standardized alphanumeric code indicating the specific failure (e.g., ERR_REQ_01).
     */
    private String errorCode;

    /**
     * Descriptive detail text of the error condition.
     */
    private String message;

    /**
     * Link back to the Batch Job Execution ID.
     */
    private Long jobId;

    /**
     * Audit timestamp recording logging occurrence.
     */
    private LocalDateTime createdTimestamp;

    /**
     * Date identifier corresponding to financial period alignment.
     */
    private LocalDate postingDate;

    public LocalDateTime getCreatedTimestamp() {
        if (createdTimestamp == null) {
            createdTimestamp = LocalDateTime.now();
        }
        return createdTimestamp;
    }
}
