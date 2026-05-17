package com.reserv.dataloader.batch.listener;

import com.fyntrac.common.entity.RefDataValidationLog;
import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.ExitStatus;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class ValidationLoggingListener implements ItemProcessListener<Object, Object>, org.springframework.batch.core.StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(ValidationLoggingListener.class);
    private final RefDataValidationLogRepository validationLogRepository;
    private Long jobId;

    private String tenantId;

    public ValidationLoggingListener(RefDataValidationLogRepository validationLogRepository) {
        this.validationLogRepository = validationLogRepository;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("V-L-L: beforeStep invoked. JobID: {}, Params: {}", stepExecution.getJobExecutionId(), stepExecution.getJobParameters());
        this.jobId = stepExecution.getJobExecutionId();
        this.tenantId = stepExecution.getJobParameters().getString("tenantId");
        log.info("V-L-L: Initialized jobId={} and tenantId={}", this.jobId, this.tenantId);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return stepExecution.getExitStatus();
    }

    @Override
    public void beforeProcess(Object item) {
    }

    @Override
    public void afterProcess(Object item, Object result) {
    }

    @Override
    public void onProcessError(Object item, Exception e) {
        log.info("V-L-L: onProcessError caught exception of type: {}", e.getClass().getName());
        if (e instanceof ItemValidationException) {
            ItemValidationException ex = (ItemValidationException) e;
            List<ItemValidationException.ValidationError> errors = ex.getValidationErrors();
            if (errors != null && !errors.isEmpty()) {
                String sourceTable = item != null ? item.getClass().getSimpleName() : "Unknown";
                List<RefDataValidationLog> logs = errors.stream().map(err -> {
                    RefDataValidationLog dbLog = new RefDataValidationLog();
                    dbLog.setSourceTable(sourceTable);
                    dbLog.setSourceColumn(err.getColumn());
                    dbLog.setSourceColumnValue(getFieldValue(item, err.getColumn()));
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

    private String getFieldValue(Object item, String columnName) {
        if (item == null || columnName == null) {
            return null;
        }

        // Special case for SubledgerMapping unvalidated enum fields stored in ThreadLocal raw context
        if (item instanceof com.fyntrac.common.entity.SubledgerMapping) {
            com.reserv.dataloader.batch.config.SubledgerMappingDataLoadConfig.RawValidationContext ctx =
                    com.reserv.dataloader.batch.config.SubledgerMappingDataLoadConfig.RAW_CONTEXT.get();
            if (ctx != null) {
                if (columnName.equalsIgnoreCase("sign")) {
                    return ctx.rawSign;
                }
                if (columnName.equalsIgnoreCase("entryType")) {
                    return ctx.rawEntryType;
                }
            }
        }

        try {
            // Try standard camelCase field name matching the columnName (case insensitive)
            for (java.lang.reflect.Field field : item.getClass().getDeclaredFields()) {
                if (field.getName().equalsIgnoreCase(columnName.trim())) {
                    field.setAccessible(true);
                    Object val = field.get(item);
                    return val != null ? val.toString() : null;
                }
            }
            // If not found in fields, try matching getter methods
            for (java.lang.reflect.Method method : item.getClass().getDeclaredMethods()) {
                if (method.getName().equalsIgnoreCase("get" + columnName.trim()) && method.getParameterCount() == 0) {
                    method.setAccessible(true);
                    Object val = method.invoke(item);
                    return val != null ? val.toString() : null;
                }
            }
        } catch (Exception ex) {
            log.warn("Failed to dynamically resolve field value for field={} on class={}", columnName, item.getClass().getName(), ex);
        }
        return null;
    }
}
