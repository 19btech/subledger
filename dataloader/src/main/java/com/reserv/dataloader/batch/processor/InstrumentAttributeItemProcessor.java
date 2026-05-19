package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.entity.factory.InstrumentAttributeFactory;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.repository.AttributesRepository;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import com.fyntrac.common.utils.DateUtil;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.validation.InstrumentAttributeValidator;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Spring Batch ItemProcessor for InstrumentAttribute CSV rows.
 *
 * <p>Validation pattern matches AggregateItemProcessor:
 * <ol>
 *   <li>Pre-load reference data into in-memory Sets once per step (@BeforeStep).</li>
 *   <li>Validate each row; collect errors without failing the job.</li>
 *   <li>Persist errors to RefDataValidationLog (tenant-scoped).</li>
 *   <li>Return {@code null} to silently skip invalid rows.</li>
 * </ol>
 */
public class InstrumentAttributeItemProcessor
        implements ItemProcessor<Map<String, Object>, InstrumentAttribute>, StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(InstrumentAttributeItemProcessor.class);

    private final InstrumentAttributeValidator validator;
    private final RefDataValidationLogRepository validationLogRepository;
    private final AttributesRepository attributesRepository;

    @Autowired
    private InstrumentAttributeFactory instrumentAttributeFactory;

    @Autowired
    private com.fyntrac.common.config.TenantDatasourceConfig tenantDatasourceConfig;

    // Step-scoped state
    private Long jobId;
    private String tenantId;

    // Preloaded reference sets (normalised to UPPER-CASE for O(1) lookup)
    private final Set<String> validAttributeIds = new HashSet<>();

    public InstrumentAttributeItemProcessor(
            InstrumentAttributeValidator validator,
            RefDataValidationLogRepository validationLogRepository,
            AttributesRepository attributesRepository) {
        this.validator = validator;
        this.validationLogRepository = validationLogRepository;
        this.attributesRepository = attributesRepository;
    }

    // ------------------------------------------------------------------
    // Step lifecycle
    // ------------------------------------------------------------------

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.tenantId = stepExecution.getJobParameters().getString("tenantId");
        this.jobId    = stepExecution.getJobExecutionId();
        log.info("Initializing InstrumentAttributeItemProcessor for tenant: {} job: {}", tenantId, jobId);

        validAttributeIds.clear();

        if (tenantId == null || tenantId.isBlank()) {
            log.error("No tenantId provided — attribute reference preload skipped.");
            return;
        }

        // Configure multi-tenant datasource
        if (tenantDatasourceConfig != null) {
            try {
                tenantDatasourceConfig.configureTenantDatabases(tenantId);
            } catch (Exception e) {
                log.error("Failed to configure tenant database for: {}", tenantId, e);
            }
        }

        // Preload valid attributeIds from the Attributes reference table
        TenantContextHolder.runWithTenant(tenantId, () -> {
            try {
                attributesRepository.findAll().forEach(attr -> {
                    if (attr.getId() != null) {
                        validAttributeIds.add(
                                InstrumentAttributeValidator.normalizeId(attr.getId()).toUpperCase());
                    }
                });
                log.info("Preloaded {} valid attribute IDs for validation.", validAttributeIds.size());
            } catch (Exception e) {
                log.error("Failed to preload attribute IDs for InstrumentAttributeItemProcessor", e);
            }
        });
    }

    // ------------------------------------------------------------------
    // Processing
    // ------------------------------------------------------------------

    @Override
    public InstrumentAttribute process(Map<String, Object> item) throws Exception {

        // 1. Validate — inside tenant context so any lazy repo call is scoped correctly
        List<ItemValidationException.ValidationError> errors;
        if (tenantId != null) {
            errors = TenantContextHolder.runWithTenant(tenantId,
                    () -> validator.validate(item, validAttributeIds));
        } else {
            errors = validator.validate(item, validAttributeIds);
        }

        // 2. Persist errors and skip row when any ERROR-severity violation found
        boolean hasError = errors.stream().anyMatch(e -> "ERROR".equals(e.getSeverity()));
        if (hasError) {
            List<com.fyntrac.common.entity.RefDataValidationLog> logs = errors.stream().map(err -> {
                com.fyntrac.common.entity.RefDataValidationLog dbLog =
                        new com.fyntrac.common.entity.RefDataValidationLog();
                dbLog.setSourceTable("InstrumentAttribute");
                dbLog.setSourceColumn(err.getColumn());
                dbLog.setSourceColumnValue(err.getValue());
                dbLog.setSeverity(err.getSeverity());
                dbLog.setErrorCode(err.getErrorCode());
                dbLog.setMessage(err.getMessage());
                dbLog.setJobId(this.jobId);
                dbLog.setErrorCategory("DATA");
                return dbLog;
            }).collect(Collectors.toList());

            if (tenantId != null) {
                TenantContextHolder.runWithTenant(tenantId, () -> {
                    validationLogRepository.saveAll(logs);
                });
            } else {
                validationLogRepository.saveAll(logs);
            }
            log.warn("Filtered row and saved {} InstrumentAttribute validation logs to DB.", logs.size());
            return null; // silently skip — no Spring Batch rollback
        }

        // 3. Map raw CSV columns to entity
        final Map<String, Object> attributes = new HashMap<>();
        Date effectiveDate = null;
        String instrumentId = "";
        String attributeId  = "";
        int postingDate = 0;

        for (Map.Entry<String, Object> entry : item.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.isBlank()) continue;
            Object value = entry.getValue();

            switch (key.toUpperCase()) {
                case "ACTIVITYUPLOADID":
                    break; // skip upload metadata

                case "EFFECTIVEDATE":
                    effectiveDate = parseDate(value);
                    break;

                case "INSTRUMENTID":
                    instrumentId = InstrumentAttributeValidator.normalizeId(toStr(value));
                    break;

                case "ATTRIBUTEID":
                    attributeId = toStr(value);  // preserve raw value (e.g. "1.0") — do NOT normalise
                    break;

                case "POSTINGDATE":
                    if (value instanceof String strDate) {
                        try {
                            Date pDate = DateUtil.parseDate(strDate.trim());
                            postingDate = DateUtil.dateInNumber(pDate);
                        } catch (Exception e) {
                            log.warn("Could not parse postingDate '{}': {}", strDate, e.getMessage());
                        }
                    }
                    break;

                default:
                    attributes.put(key, inferType(value));
                    break;
            }
        }

        // 4. Build entity via factory (assigns versionId / sequenceId)
        InstrumentAttribute result = instrumentAttributeFactory.create(
                this.tenantId,
                instrumentId,
                attributeId,
                effectiveDate,
                0,
                postingDate,
                Source.ETL,
                attributes);

        // 5. Ensure MongoDB _id exists before writer uses it
        if (result.getId() == null) {
            result.setId(new ObjectId().toString());
        }

        // Attach empty error list so downstream code can safely call getValidationErrors()
        result.setValidationErrors(new ArrayList<>());

        return result;
    }

    // ------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------

    private static Date parseDate(Object value) {
        if (value instanceof String strVal) {
            String trimmed = strVal.trim();
            // Try MM/dd/yyyy first (most common CSV format)
            for (String pattern : new String[]{"M/d/yyyy", "MM/dd/yyyy", "M/dd/yyyy"}) {
                try {
                    LocalDate ld = LocalDate.parse(trimmed, DateTimeFormatter.ofPattern(pattern));
                    return Date.from(ld.atStartOfDay(ZoneOffset.UTC).toInstant());
                } catch (Exception ignored) {}
            }
            // Fallback to DateUtil
            try {
                return DateUtil.parseDate(trimmed);
            } catch (Exception ignored) {}
        }
        return null;
    }

    private static String toStr(Object value) {
        return value != null ? String.valueOf(value).trim() : "";
    }

    private static Object inferType(Object value) {
        if (value == null) return null;
        if (value instanceof String strVal) {
            String t = strVal.trim();
            if (t.equalsIgnoreCase("true") || t.equalsIgnoreCase("false"))
                return Boolean.parseBoolean(t);
            try {
                return t.contains(".") ? Double.parseDouble(t) : Long.parseLong(t);
            } catch (NumberFormatException ignored) {}
            try {
                LocalDate ld = LocalDate.parse(t, DateTimeFormatter.ofPattern("MM/dd/yyyy"));
                return Date.from(ld.atStartOfDay(ZoneOffset.UTC).toInstant());
            } catch (Exception ignored) {}
            return t;
        }
        return value;
    }
}