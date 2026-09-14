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

/**
 * Spring Batch ItemProcessor for AccountTypes (accounttype.csv) rows.
 *
 * <p>Validation pattern matches {@link TransactionsItemProcessor} / {@link AttributesItemProcessor}:
 * <ol>
 *   <li>Pre-load reference data (existing DB records) into in-memory sets once per step ({@code beforeStep}),
 *       so re-uploading a file whose rows were already loaded in a prior job run is caught instead of
 *       being silently re-inserted as a duplicate.</li>
 *   <li>Validate each row — collect errors without failing the job.</li>
 *   <li>Persist errors to {@code RefDataValidationLog}.</li>
 *   <li>Return {@code null} to silently skip invalid/duplicate rows.</li>
 * </ol>
 *
 * <p>Implements {@link StepExecutionListener} directly rather than relying on the {@code @BeforeStep}
 * annotation, matching the pattern already used by {@code SubledgerMappingItemProcessor}. The
 * actual bug that made {@code beforeStep} never fire was the {@code accountTypeItemProcessor()}
 * {@code @Bean} method in {@code AccountTypeDataLoadConfig} being declared to return the
 * {@code ItemProcessor} interface instead of this concrete class — see the comment there for the
 * full explanation. This is the class actually wired into {@code AccountTypeUploadService}
 * (bean name collision with the near-identical, but never-invoked, {@code AccountTypesItemProcessor}).
 */
@Slf4j
public class AccountTypeItemProcessor implements ItemProcessor<AccountTypes, AccountTypes>, StepExecutionListener {

    private final AccountTypesValidator validator;
    private final AccountTypesRepository accountTypesRepository;
    private final RefDataValidationLogRepository validationLogRepository;

    // In-memory structures for file-level validation, preloaded with existing DB
    // records in beforeStep so a repeat upload of already-loaded data is caught too.
    private final Set<String> seenSubTypes = ConcurrentHashMap.newKeySet();
    private final Map<String, String> subTypeToTypeMap = new ConcurrentHashMap<>();

    private Long jobId;
    private String tenantId;

    public AccountTypeItemProcessor(
            AccountTypesValidator validator,
            AccountTypesRepository accountTypesRepository,
            RefDataValidationLogRepository validationLogRepository) {
        this.validator = validator;
        this.accountTypesRepository = accountTypesRepository;
        this.validationLogRepository = validationLogRepository;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        // Use the app-level "run.id" job parameter (not Spring Batch's internal
        // JobExecutionId) so RefDataValidationLog.jobId matches ActivityLog.jobId,
        // which is what callers/UI actually have on hand to correlate an upload.
        this.jobId = stepExecution.getJobParameters().getLong("run.id");
        this.tenantId = stepExecution.getJobParameters().getString("tenantId");

        this.seenSubTypes.clear();
        this.subTypeToTypeMap.clear();

        log.info("Initializing AccountTypeItemProcessor for tenant: {} and jobId: {}", tenantId, jobId);

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
        List<ItemValidationException.ValidationError> errors = validator.validate(item, seenSubTypes, subTypeToTypeMap);

        boolean hasError = errors.stream().anyMatch(e -> "ERROR".equals(e.getSeverity()));

        if (hasError) {
            List<RefDataValidationLog> dbLogs = errors.stream().map(err -> {
                RefDataValidationLog logEntity = new RefDataValidationLog();
                logEntity.setSourceTable("AccountTypes");
                logEntity.setSourceColumn(err.getColumn());
                logEntity.setSourceColumnValue(err.getValue());
                logEntity.setSeverity(err.getSeverity());
                logEntity.setErrorCode(err.getErrorCode());
                logEntity.setMessage(err.getMessage());
                logEntity.setJobId(this.jobId);
                logEntity.setErrorCategory("DATA");
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

            log.warn("Filtered AccountTypes record and saved {} validation logs to DB.", dbLogs.size());
            return null; // Silently filter out invalid/duplicate rows to prevent Spring Batch rollbacks
        }

        // Record accepted: update in-memory state so later rows in the same file are
        // also checked against it (in-file dedup, on top of the DB preload above).
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
