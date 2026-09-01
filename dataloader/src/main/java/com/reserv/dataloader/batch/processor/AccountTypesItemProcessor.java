package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.entity.RefDataValidationLog;
import com.fyntrac.common.repository.AccountTypesRepository;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.validation.AccountTypesValidator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

// Implements StepExecutionListener directly (matching SubledgerMappingItemProcessor) for
// consistency. Unlike TransactionsItemProcessor/AttributesItemProcessor, this class's @Bean
// factory method (in AggregationDataLoadConfig) already returns the concrete
// AccountTypesItemProcessor type rather than the bare ItemProcessor interface, so its @StepScope
// proxy was always a proper CGLIB subclass and beforeStep() already fired correctly either way.
@Slf4j
public class AccountTypesItemProcessor implements ItemProcessor<AccountTypes, AccountTypes>, StepExecutionListener {

    private final AccountTypesValidator validator;
    private final AccountTypesRepository accountTypesRepository;
    private final RefDataValidationLogRepository validationLogRepository;

    // In-memory structures for file-level validation, preloaded with existing DB
    // records in beforeStep so a repeat upload of already-loaded data is caught too.
    private final Set<String> seenSubTypes = ConcurrentHashMap.newKeySet();
    private final Map<String, String> subTypeToTypeMap = new ConcurrentHashMap<>();

    private Long jobId;
    private String tenantId;

    public AccountTypesItemProcessor(AccountTypesValidator validator, AccountTypesRepository accountTypesRepository, RefDataValidationLogRepository validationLogRepository) {
        this.validator = validator;
        this.accountTypesRepository = accountTypesRepository;
        this.validationLogRepository = validationLogRepository;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        this.jobId = stepExecution.getJobExecutionId();
        this.tenantId = stepExecution.getJobParameters().getString("tenantId");

        // Reset state for new step execution
        this.seenSubTypes.clear();
        this.subTypeToTypeMap.clear();

        log.info("Initializing AccountTypesItemProcessor for tenant: {} and jobId: {}", tenantId, jobId);

        if (this.tenantId == null || this.tenantId.trim().isEmpty() || this.accountTypesRepository == null) {
            return;
        }

        // Preload already-persisted AccountTypes so a record re-uploaded in a later
        // file/job is flagged as a duplicate instead of silently passing through.
        TenantContextHolder.runWithTenant(this.tenantId, () -> {
            try {
                accountTypesRepository.findAll().forEach(existing -> {
                    if (existing.getAccountSubType() != null) {
                        String trimmedSubType = existing.getAccountSubType().trim().toUpperCase();
                        seenSubTypes.add(trimmedSubType);
                        if (existing.getAccountType() != null) {
                            subTypeToTypeMap.put(trimmedSubType, existing.getAccountType().name());
                        }
                    }
                });
                log.info("Preloaded {} existing account sub types for validation.", seenSubTypes.size());
            } catch (Exception e) {
                log.error("Failed to preload existing AccountTypes for tenant {} in setup stage", this.tenantId, e);
            }
        });
    }

    @Override
    public AccountTypes process(AccountTypes item) throws Exception {
        // Execute validation logic
        List<ItemValidationException.ValidationError> errors = validator.validate(item, seenSubTypes, subTypeToTypeMap);
        
        boolean hasError = errors.stream().anyMatch(e -> "ERROR".equals(e.getSeverity()));

        if (hasError) {
            // Log errors to database
            List<RefDataValidationLog> dbLogs = errors.stream().map(err -> {
                RefDataValidationLog logEntity = new RefDataValidationLog();
                logEntity.setSourceTable("AccountTypes");
                logEntity.setSourceColumn(err.getColumn());
                logEntity.setSourceColumnValue(err.getValue());
                logEntity.setSeverity(err.getSeverity());
                logEntity.setErrorCode(err.getErrorCode());
                logEntity.setMessage(err.getMessage());
                logEntity.setJobId(this.jobId);
                logEntity.setErrorCategory("DATA_VALIDATION");
                logEntity.setValidationType(com.fyntrac.common.enums.ValidationType.JOURNAL_MAPPING);
                return logEntity;
            }).collect(Collectors.toList());

            if (this.tenantId != null) {
                TenantContextHolder.runWithTenant(this.tenantId, () -> {
                    validationLogRepository.saveAll(dbLogs);
                });
            } else {
                validationLogRepository.saveAll(dbLogs);
            }
            
            log.warn("Skipping AccountTypes record due to {} validation errors", dbLogs.size());
            return null; // Skip record
        }

        // Record successfully processed: update in-memory state
        if (item.getAccountSubType() != null) {
            String trimmedSubType = item.getAccountSubType().trim().toUpperCase();
            seenSubTypes.add(trimmedSubType);
            if (item.getAccountType() != null) {
                subTypeToTypeMap.put(trimmedSubType, item.getAccountType().name());
            }
        }

        return item;
    }
}
