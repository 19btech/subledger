package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.RefDataValidationLog;
import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.enums.ErrorCode;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.validation.TransactionValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionsItemProcessor implements ItemProcessor<Transactions, Transactions> {

    private static final Logger log = LoggerFactory.getLogger(TransactionsItemProcessor.class);
    private final TransactionValidator validator;
    private final com.fyntrac.common.repository.RefDataValidationLogRepository validationLogRepository;
    private final com.fyntrac.common.repository.MemcachedRepository memcachedRepository;
    private final Set<String> seenTransactionNames = ConcurrentHashMap.newKeySet();
    private final Set<String> existingTransactionNames = new java.util.HashSet<>();
    private Long jobId;
    private String tenantId;

    public TransactionsItemProcessor(
            TransactionValidator validator,
            com.fyntrac.common.repository.RefDataValidationLogRepository validationLogRepository,
            com.fyntrac.common.repository.MemcachedRepository memcachedRepository) {
        this.validator = validator;
        this.validationLogRepository = validationLogRepository;
        this.memcachedRepository = memcachedRepository;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.seenTransactionNames.clear();
        this.existingTransactionNames.clear();
        this.tenantId = stepExecution.getJobParameters().getString("tenantId");
        this.jobId = stepExecution.getJobExecutionId();

        if (this.tenantId != null && !this.tenantId.trim().isEmpty()) {
            // 1. Invalidate stale cache before starting step
            if (this.memcachedRepository != null) {
                String cacheKey = com.fyntrac.common.utils.Key.transactionsKey(this.tenantId);
                try {
                    log.info("Purging stale transaction cache for tenant {} before loading data: {}", this.tenantId, cacheKey);
                    this.memcachedRepository.delete(cacheKey);
                } catch (Exception e) {
                    log.warn("Failed to purge stale transaction cache key {}: {}", cacheKey, e.getMessage());
                }
            }

            // 2. Direct MongoDB Preload in physical tenant context to prevent N+1 queries and duplicate issues
            com.fyntrac.common.config.TenantContextHolder.runWithTenant(this.tenantId, () -> {
                try {
                    validator.getTransactionService().getAll().forEach(tx -> {
                        if (tx.getName() != null) {
                            existingTransactionNames.add(tx.getName().trim().toUpperCase());
                        }
                    });
                    log.info("Preloaded {} existing transaction configurations for validation.", existingTransactionNames.size());
                } catch (Exception e) {
                    log.error("Failed to preload existing transactions for tenant {} in setup stage", this.tenantId, e);
                }
            });
        }
    }

    @Override
    public Transactions process(Transactions item) throws Exception {
        List<ItemValidationException.ValidationError> itemLogs = validator.validate(item, existingTransactionNames);

        boolean hasError = itemLogs.stream().anyMatch(validationLog -> "ERROR".equals(validationLog.getSeverity()));
        String name = item.getName();

        if (hasError) {
            List<com.fyntrac.common.entity.RefDataValidationLog> logs = itemLogs.stream().map(err -> {
                com.fyntrac.common.entity.RefDataValidationLog dbLog = new com.fyntrac.common.entity.RefDataValidationLog();
                dbLog.setSourceTable("Transactions");
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
            log.warn("Filtered row and saved {} transaction validation logs to DB.", logs.size());
            return null; // Silently filter out invalid rows to prevent Spring Batch transaction rollbacks
        }

        if (name != null) {
            if (!seenTransactionNames.add(name)) {
                log.warn("Skipping duplicate transaction record found in the file: {}", name);
                return null; // Silently filter out file-level duplicates without throwing exceptions
            }
        }

        if (!itemLogs.isEmpty()) {
            itemLogs.forEach(validationLog -> {
                log.warn("Validation Warning: {}", validationLog.getMessage());
            });
        }

        return item;
    }
}
