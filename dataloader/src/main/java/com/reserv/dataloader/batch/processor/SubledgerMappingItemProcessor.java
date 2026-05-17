package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.RefDataValidationLog;
import com.fyntrac.common.entity.SubledgerMapping;
import com.fyntrac.common.repository.AccountTypesRepository;
import com.fyntrac.common.repository.TransactionsRepository;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.validation.SubledgerMappingValidator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import com.fyntrac.common.repository.RefDataValidationLogRepository;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class SubledgerMappingItemProcessor implements ItemProcessor<SubledgerMapping, SubledgerMapping>, org.springframework.batch.core.StepExecutionListener {

    private final SubledgerMappingValidator validator;
    private final TransactionsRepository transactionsRepository;
    private final AccountTypesRepository accountTypesRepository;
    private final RefDataValidationLogRepository validationLogRepository;

    private Long jobId;
    private String tenantId;

    private final Set<String> validTransactionNames = new HashSet<>();
    private final Set<String> validAccountSubTypes = new HashSet<>();

    public SubledgerMappingItemProcessor(
            SubledgerMappingValidator validator,
            TransactionsRepository transactionsRepository,
            AccountTypesRepository accountTypesRepository,
            RefDataValidationLogRepository validationLogRepository) {
        this.validator = validator;
        this.transactionsRepository = transactionsRepository;
        this.accountTypesRepository = accountTypesRepository;
        this.validationLogRepository = validationLogRepository;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        this.tenantId = stepExecution.getJobParameters().getString("tenantId");
        this.jobId = stepExecution.getJobExecutionId();

        log.info("S-M-I-P: beforeStep invoked. tenantId={}, jobId={}", this.tenantId, this.jobId);

        log.info("Initializing preloaded caches for SubledgerMappingItemProcessor for tenant: {}", this.tenantId);

        validTransactionNames.clear();
        validAccountSubTypes.clear();
        validator.clearState();

        if (this.tenantId == null || this.tenantId.trim().isEmpty()) {
            log.error("Critical: No tenantId provided in JobParameters for SubledgerMappingItemProcessor step setup!");
            return;
        }

        TenantContextHolder.runWithTenant(this.tenantId, () -> {
            try {
                // Preload Transaction Names
                transactionsRepository.findAll().forEach(tx -> {
                    if (tx.getName() != null) {
                        validTransactionNames.add(tx.getName().trim());
                    }
                });
                log.info("Preloaded {} valid transaction names.", validTransactionNames.size());
            } catch (Exception e) {
                log.error("Failed to preload transaction names", e);
            }

            try {
                // Preload Account Sub Types
                accountTypesRepository.findAll().forEach(acc -> {
                    if (acc.getAccountSubType() != null) {
                        validAccountSubTypes.add(acc.getAccountSubType().trim());
                    }
                });
                log.info("Preloaded {} valid account sub types.", validAccountSubTypes.size());
            } catch (Exception e) {
                log.error("Failed to preload account sub types", e);
            }
        });
    }

    @Override
    public SubledgerMapping process(SubledgerMapping item) throws Exception {
        validator.validate(item, validTransactionNames, validAccountSubTypes);
        return item;
    }


}
