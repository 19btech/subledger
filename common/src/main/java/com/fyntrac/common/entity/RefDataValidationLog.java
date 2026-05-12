package com.fyntrac.common.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Entity to log validation errors encountered during reference data processing.
 */
@Entity
@Table(name = "REF_DATA_VALIDATION_LOG")
public class RefDataValidationLog {

    /**
     * Primary key, Auto-generated sequence.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "error_id")
    private Long errorId;

    /**
     * Identifies the name of the source table being processed.
     */
    @Column(name = "source_table", length = 100)
    private String sourceTable;

    /**
     * Identifies the specific column yielding the validation error.
     */
    @Column(name = "source_column", length = 100)
    private String sourceColumn;

    /**
     * Represents record sequence number in the input.
     */
    @Column(name = "row_num")
    private Long rowNum;

    /**
     * Categorizes error source: DATA or PROCESSING.
     */
    @Column(name = "error_category")
    private String errorCategory;

    /**
     * Classification of error impact: ERROR or WARNING.
     */
    @Column(name = "severity")
    private String severity;

    /**
     * Standardized alphanumeric code indicating the specific failure (e.g., ERR_REQ_01).
     */
    @Column(name = "error_code", length = 50)
    private String errorCode;

    /**
     * Descriptive detail text of the error condition.
     */
    @Column(name = "message", length = 1000)
    private String message;

    /**
     * Link back to the Batch Job Execution ID.
     */
    @Column(name = "job_id")
    private Long jobId;

    /**
     * Audit timestamp recording logging occurrence. Set once at creation.
     */
    @Column(name = "created_timestamp", updatable = false)
    private LocalDateTime createdTimestamp;

    /**
     * Date identifier corresponding to financial period alignment.
     */
    @Column(name = "posting_date")
    private LocalDate postingDate;

    @PrePersist
    protected void onCreate() {
        if (this.createdTimestamp == null) {
            this.createdTimestamp = LocalDateTime.now();
        }
    }

    /**
     * Default No-arguments Constructor.
     */
    public RefDataValidationLog() {
    }

    /**
     * All-arguments Constructor.
     */
    public RefDataValidationLog(Long errorId, String sourceTable, String sourceColumn, Long rowNum, 
                               String errorCategory, String severity, String errorCode, String message, 
                               Long jobId, LocalDateTime createdTimestamp, LocalDate postingDate) {
        this.errorId = errorId;
        this.sourceTable = sourceTable;
        this.sourceColumn = sourceColumn;
        this.rowNum = rowNum;
        this.errorCategory = errorCategory;
        this.severity = severity;
        this.errorCode = errorCode;
        this.message = message;
        this.jobId = jobId;
        this.createdTimestamp = createdTimestamp;
        this.postingDate = postingDate;
    }

    // Getters and Setters

    public Long getErrorId() {
        return errorId;
    }

    public void setErrorId(Long errorId) {
        this.errorId = errorId;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getSourceColumn() {
        return sourceColumn;
    }

    public void setSourceColumn(String sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    public Long getRowNum() {
        return rowNum;
    }

    public void setRowNum(Long rowNum) {
        this.rowNum = rowNum;
    }

    public String getErrorCategory() {
        return errorCategory;
    }

    public void setErrorCategory(String errorCategory) {
        this.errorCategory = errorCategory;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public LocalDateTime getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(LocalDateTime createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public LocalDate getPostingDate() {
        return postingDate;
    }

    public void setPostingDate(LocalDate postingDate) {
        this.postingDate = postingDate;
    }
}
