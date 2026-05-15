package com.reserv.dataloader.batch.listener;

import com.fyntrac.common.entity.RefDataValidationLog;
import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class ValidationLoggingListener implements ItemProcessListener<Object, Object> {

    private static final Logger log = LoggerFactory.getLogger(ValidationLoggingListener.class);
    private final RefDataValidationLogRepository validationLogRepository;
    private Long jobId;

    private String tenantId;

    public ValidationLoggingListener(RefDataValidationLogRepository validationLogRepository) {
        this.validationLogRepository = validationLogRepository;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.jobId = stepExecution.getJobExecutionId();
        this.tenantId = stepExecution.getJobParameters().getString("tenantId");
    }

    @Override
    public void beforeProcess(Object item) {
    }

    @Override
    public void afterProcess(Object item, Object result) {
    }

    @Override
    public void onProcessError(Object item, Exception e) {
        if (e instanceof ItemValidationException) {
            ItemValidationException ex = (ItemValidationException) e;
            List<ItemValidationException.ValidationError> errors = ex.getValidationErrors();
            if (errors != null && !errors.isEmpty()) {
                String sourceTable = item != null ? item.getClass().getSimpleName() : "Unknown";
                List<RefDataValidationLog> logs = errors.stream().map(err -> {
                    RefDataValidationLog dbLog = new RefDataValidationLog();
                    dbLog.setSourceTable(sourceTable);
                    dbLog.setSourceColumn(err.getColumn());
                    dbLog.setSeverity(err.getSeverity());
                    dbLog.setErrorCode(err.getErrorCode());
                    dbLog.setMessage(err.getMessage());
                    dbLog.setJobId(this.jobId);
                    dbLog.setErrorCategory("DATA");
                    return dbLog;
                }).collect(Collectors.toList());

                if (this.tenantId != null) {
                    com.fyntrac.common.config.TenantContextHolder.runWithTenant(this.tenantId, () -> {
                        validationLogRepository.saveAll(logs);
                    });
                } else {
                    validationLogRepository.saveAll(logs);
                }
                log.debug("Saved {} validation logs for item in table {}.", logs.size(), sourceTable);
            }
        } else {
            log.error("Item processing failed due to unexpected error", e);
        }
    }
}
