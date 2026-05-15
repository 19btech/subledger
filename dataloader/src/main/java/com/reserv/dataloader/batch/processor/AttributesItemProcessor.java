package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.entity.RefDataValidationLog;
import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.repository.AttributesRepository;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.validation.AttributesValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class AttributesItemProcessor implements ItemProcessor<Attributes, Attributes> {

    private static final Logger log = LoggerFactory.getLogger(AttributesItemProcessor.class);

    private final AttributesValidator validator;
    private final AttributesRepository attributesRepository;
    private final RefDataValidationLogRepository validationLogRepository;

    private final Set<String> seenAttributeNames = ConcurrentHashMap.newKeySet();
    private final Set<String> existingAttributeNames = new HashSet<>();
    private Long jobId;
    private String tenantId;

    public AttributesItemProcessor(
            AttributesValidator validator,
            AttributesRepository attributesRepository,
            RefDataValidationLogRepository validationLogRepository) {
        this.validator = validator;
        this.attributesRepository = attributesRepository;
        this.validationLogRepository = validationLogRepository;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.seenAttributeNames.clear();
        this.existingAttributeNames.clear();
        this.jobId = stepExecution.getJobExecutionId();
        this.tenantId = stepExecution.getJobParameters().getString("tenantId");

        log.info("Initializing preloaded caches for AttributesItemProcessor for tenant: {}", this.tenantId);

        if (this.tenantId != null && !this.tenantId.trim().isEmpty() && this.attributesRepository != null) {
            // Optimization: Direct MongoDB Preload in physical tenant context to prevent N+1 queries
            TenantContextHolder.runWithTenant(this.tenantId, () -> {
                try {
                    attributesRepository.findAll().forEach(attr -> {
                        if (attr.getAttributeName() != null) {
                            existingAttributeNames.add(attr.getAttributeName().trim().toUpperCase());
                        }
                    });
                    log.info("Preloaded {} existing attribute definitions for validation.", existingAttributeNames.size());
                } catch (Exception e) {
                    log.error("Failed to preload existing attributes for tenant {} in setup stage", this.tenantId, e);
                }
            });
        }
    }

    @Override
    public Attributes process(Attributes item) throws Exception {
        // 1. Execute core schema constraint & DB existence checks
        List<ItemValidationException.ValidationError> errors = validator.validate(item, existingAttributeNames);

        boolean hasError = errors.stream().anyMatch(e -> "ERROR".equals(e.getSeverity()));
        String attributeName = item.getAttributeName();

        if (hasError) {
            List<RefDataValidationLog> logs = errors.stream().map(err -> {
                RefDataValidationLog dbLog = new RefDataValidationLog();
                dbLog.setSourceTable("Attributes");
                dbLog.setSourceColumn(err.getColumn());
                dbLog.setSeverity(err.getSeverity());
                dbLog.setErrorCode(err.getErrorCode());
                dbLog.setMessage(err.getMessage());
                dbLog.setJobId(this.jobId);
                dbLog.setErrorCategory("DATA");
                return dbLog;
            }).collect(Collectors.toList());

            if (this.tenantId != null) {
                TenantContextHolder.runWithTenant(this.tenantId, () -> {
                    validationLogRepository.saveAll(logs);
                });
            } else {
                validationLogRepository.saveAll(logs);
            }
            log.warn("Filtered invalid Attributes record and saved {} validation logs to DB.", logs.size());
            return null; // Return null to silently skip invalid record in the fault-tolerant batch pipeline
        }

        // 2. Check duplicates within the current file (CSV Deduplication)
        if (attributeName != null) {
            String rawName = attributeName.trim().toUpperCase();
            if (!seenAttributeNames.add(rawName)) {
                log.warn("Skipping duplicate attribute configuration record found within same file: {}", rawName);
                // Add duplicate tracking error log if requested, or simply filter out silently
                RefDataValidationLog dbLog = new RefDataValidationLog();
                dbLog.setSourceTable("Attributes");
                dbLog.setSourceColumn("ATTRIBUTENAME");
                dbLog.setSeverity("ERROR");
                dbLog.setErrorCode(ErrorCode.ERR_DUP_01.name());
                dbLog.setMessage("Duplicate attribute configuration found in file: " + attributeName);
                dbLog.setJobId(this.jobId);
                dbLog.setErrorCategory("DATA");
                
                if (this.tenantId != null) {
                    TenantContextHolder.runWithTenant(this.tenantId, () -> {
                        validationLogRepository.save(dbLog);
                    });
                } else {
                    validationLogRepository.save(dbLog);
                }
                return null; 
            }
        }

        // 3. Construct fresh Entity for Persistence mapping
        final Attributes processedAttribute = new Attributes();
        processedAttribute.setAttributeName(item.getAttributeName().trim());
        processedAttribute.setUserField(item.getUserField());
        processedAttribute.setDataType(item.getDataType());
        processedAttribute.setIsReclassable(item.getIsReclassable());
        processedAttribute.setIsVersionable(item.getIsVersionable());
        processedAttribute.setIsNullable(item.getIsNullable());
        processedAttribute.setSequenceId(item.getSequenceId());

        return processedAttribute;
    }
}
