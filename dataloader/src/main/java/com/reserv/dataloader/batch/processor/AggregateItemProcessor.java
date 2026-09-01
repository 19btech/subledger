package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.Aggregation;
import com.fyntrac.common.enums.ErrorCode;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.fyntrac.common.service.TransactionService;
import com.reserv.dataloader.validation.AggregationValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AggregateItemProcessor implements ItemProcessor<Aggregation, Aggregation> {

    private static final Logger log = LoggerFactory.getLogger(AggregateItemProcessor.class);
    private final AggregationValidator validator;
    private final TransactionService transactionService;
    private final AggregationService aggregationService;
    private final com.fyntrac.common.repository.RefDataValidationLogRepository validationLogRepository;
    private Long jobId;
    private String tenantId;

    private final Set<String> validTransactionNames = new HashSet<>();
    private final Set<String> existingMetricTransactionKeys = new HashSet<>();
    private final Set<String> seenMetricTransactionKeys = ConcurrentHashMap.newKeySet();

    public AggregateItemProcessor(
            AggregationValidator validator,
            TransactionService transactionService,
            AggregationService aggregationService,
            com.fyntrac.common.repository.RefDataValidationLogRepository validationLogRepository) {
        this.validator = validator;
        this.transactionService = transactionService;
        this.aggregationService = aggregationService;
        this.validationLogRepository = validationLogRepository;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.tenantId = stepExecution.getJobParameters().getString("tenantId");
        // Use the app-level "run.id" job parameter (not Spring Batch's internal
        // JobExecutionId) so RefDataValidationLog.jobId matches ActivityLog.jobId,
        // which is what callers/UI actually have on hand to correlate an upload.
        this.jobId = stepExecution.getJobParameters().getLong("run.id");
        log.info("Initializing preloaded caches for AggregateItemProcessor for tenant: {}", this.tenantId);

        validTransactionNames.clear();
        existingMetricTransactionKeys.clear();
        seenMetricTransactionKeys.clear();

        if (this.tenantId == null || this.tenantId.trim().isEmpty()) {
            log.error("Critical: No tenantId provided in JobParameters for AggregateItemProcessor step setup!");
            return;
        }

        com.fyntrac.common.config.TenantContextHolder.runWithTenant(this.tenantId, () -> {
            // 1. Optimize Performance: Preload Valid Transactions (O(1) Access)
            try {
                transactionService.getAll().forEach(tx -> {
                    if (tx.getName() != null) {
                        validTransactionNames.add(tx.getName().trim().toUpperCase());
                    }
                });
                log.info("Preloaded {} valid transaction names for validation.", validTransactionNames.size());
            } catch (Exception e) {
                log.error("Critical: Failed to preload valid transactions in AggregateItemProcessor step setup", e);
            }

            // 2. Optimize Performance: Preload Existing Metrics and Transaction composite keys from database to prevent
            // N+1 query times (O(1) Access)
            try {
                aggregationService.fetchAll().forEach(agg -> {
                    if (agg.getMetricName() != null && agg.getTransactionName() != null) {
                        String key = agg.getMetricName().trim().toUpperCase() + ":" + agg.getTransactionName().trim().toUpperCase();
                        existingMetricTransactionKeys.add(key);
                    }
                });
                log.info("Preloaded {} existing metric-transaction composite keys for validation.", existingMetricTransactionKeys.size());
            } catch (Exception e) {
                log.error("Critical: Failed to preload existing metrics in AggregateItemProcessor step setup", e);
            }
        });
    }

    @Override
    public Aggregation process(Aggregation item) throws Exception {
        // Execute DB-existence and pattern validation checks
        List<ItemValidationException.ValidationError> errors = validator.validate(
                item,
                validTransactionNames,
                existingMetricTransactionKeys);

        boolean hasError = errors.stream().anyMatch(e -> "ERROR".equals(e.getSeverity()));
        String metricName = item.getMetricName();
        String transactionName = item.getTransactionName();

        if (hasError) {
            List<com.fyntrac.common.entity.RefDataValidationLog> logs = errors.stream().map(err -> {
                com.fyntrac.common.entity.RefDataValidationLog dbLog = new com.fyntrac.common.entity.RefDataValidationLog();
                dbLog.setSourceTable("Aggregation");
                dbLog.setSourceColumn(err.getColumn());
                dbLog.setSourceColumnValue(err.getValue());
                dbLog.setSeverity(err.getSeverity());
                dbLog.setErrorCode(err.getErrorCode());
                dbLog.setMessage(err.getMessage());
                dbLog.setJobId(this.jobId);
                dbLog.setErrorCategory("DATA");
                dbLog.setValidationType(com.fyntrac.common.enums.ValidationType.ACCOUNTING_RULES);
                return dbLog;
            }).collect(java.util.stream.Collectors.toList());

            if (this.tenantId != null) {
                com.fyntrac.common.config.TenantContextHolder.runWithTenant(this.tenantId, () -> {
                    validationLogRepository.saveAll(logs);
                });
            } else {
                validationLogRepository.saveAll(logs);
            }
            log.warn("Filtered row and saved {} aggregation validation logs to DB.", logs.size());
            return null; // Silently filter out invalid rows to prevent Spring Batch transaction rollbacks
        }

        // Validate Uniqueness within the CSV File itself
        if (metricName != null && transactionName != null) {
            String rawMetric = metricName.trim().toUpperCase();
            String rawTxName = transactionName.trim().toUpperCase();
            String compositeKey = rawMetric + ":" + rawTxName;
            if (!seenMetricTransactionKeys.add(compositeKey)) {
                log.warn("Skipping duplicate aggregation record found in the file: {}", compositeKey);
                return null; // Silently filter out file-level duplicates without throwing exceptions
            }
        }

        // Transformation step - trim and upper case names for clean data loads
        final Aggregation processedAggregation = new Aggregation();
        processedAggregation.setTransactionName(item.getTransactionName().trim().toUpperCase());
        processedAggregation.setMetricName(item.getMetricName().trim().toUpperCase());

        return processedAggregation;
    }
}
