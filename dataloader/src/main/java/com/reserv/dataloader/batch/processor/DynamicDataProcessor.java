package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.CustomTableColumn;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.enums.ValidationType;
import com.fyntrac.common.repository.InstrumentAttributeRepository;
import com.fyntrac.common.utils.DateUtil;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.service.ActivityValidationLogService;
import com.reserv.dataloader.validation.DynamicTableValidator;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.transform.FieldSet;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Spring Batch ItemProcessor for dynamic (customer-driven) table CSV rows.
 *
 * <p>Validation pattern matches {@code AggregateItemProcessor}:
 * <ol>
 *   <li>Pre-load reference data into in-memory Sets once per step ({@code @BeforeStep}).</li>
 *   <li>Validate each row via {@link DynamicTableValidator} (metadata-driven).</li>
 *   <li>Persist errors to {@code RefDataValidationLog} (tenant-scoped).</li>
 *   <li>Return {@code null} to silently skip invalid rows — no job failure.</li>
 * </ol>
 */
public class DynamicDataProcessor
        implements ItemProcessor<FieldSet, Document>, StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(DynamicDataProcessor.class);

    private final CustomTableDefinition tableDefinition;
    private final DynamicTableValidator validator;
    private final InstrumentAttributeRepository instrumentAttributeRepository;
    private final ActivityValidationLogService validationLogService;

    // Step-scoped state
    private Long jobId;
    private String tenantId;
    private final AtomicLong rowCounter = new AtomicLong(0);

    // Preloaded reference sets (UPPER-CASE keys for O(1) lookup)
    private final Set<String> validInstrumentIds = new HashSet<>();
    private final Set<String> validAttributeIds  = new HashSet<>();

    // In-file duplicate key tracking
    private final Set<String> seenRowKeys = ConcurrentHashMap.newKeySet();

    public DynamicDataProcessor(CustomTableDefinition tableDefinition,
                                DynamicTableValidator validator,
                                InstrumentAttributeRepository instrumentAttributeRepository,
                                ActivityValidationLogService validationLogService) {
        this.tableDefinition = tableDefinition;
        this.validator = validator;
        this.instrumentAttributeRepository = instrumentAttributeRepository;
        this.validationLogService = validationLogService;
    }

    // ------------------------------------------------------------------
    // Step lifecycle — matches AggregateItemProcessor.beforeStep()
    // ------------------------------------------------------------------

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.tenantId = stepExecution.getJobParameters().getString("tenantId");
        this.jobId    = stepExecution.getJobExecutionId();
        this.rowCounter.set(0);
        log.info("Initializing DynamicDataProcessor for table='{}' tenant='{}' job={}",
                tableDefinition.getTableName(), tenantId, jobId);

        validInstrumentIds.clear();
        validAttributeIds.clear();
        seenRowKeys.clear();

        if (tenantId == null || tenantId.isBlank()) {
            log.error("No tenantId provided — reference preload skipped.");
            return;
        }

        TenantContextHolder.runWithTenant(tenantId, () -> {
            try {
                instrumentAttributeRepository.findAll().forEach(ia -> {
                    if (ia.getInstrumentId() != null) {
                        validInstrumentIds.add(
                                DynamicTableValidator.normalizeId(ia.getInstrumentId()).toUpperCase());
                    }
                    if (ia.getAttributeId() != null) {
                        validAttributeIds.add(
                                DynamicTableValidator.normalizeId(ia.getAttributeId()).toUpperCase());
                    }
                });
                log.info("Preloaded {} instrumentIds and {} attributeIds for DynamicDataProcessor.",
                        validInstrumentIds.size(), validAttributeIds.size());
            } catch (Exception e) {
                log.error("Failed to preload instrument/attribute IDs for DynamicDataProcessor", e);
            }
        });
    }

    // ------------------------------------------------------------------
    // Processing — matches AggregateItemProcessor.process()
    // ------------------------------------------------------------------

    @Override
    public Document process(FieldSet fieldSet) throws Exception {

        // 1. Validate inside tenant context
        long row = rowCounter.incrementAndGet();
        List<ItemValidationException.ValidationError> errors;
        if (tenantId != null) {
            errors = TenantContextHolder.runWithTenant(tenantId,
                    () -> validator.validate(fieldSet, tableDefinition,
                            seenRowKeys, validInstrumentIds, validAttributeIds));
        } else {
            errors = validator.validate(fieldSet, tableDefinition,
                    seenRowKeys, validInstrumentIds, validAttributeIds);
        }

        // 2. Persist errors to activity_data_validation_log and skip row
        boolean hasError = errors.stream().anyMatch(e -> "ERROR".equals(e.getSeverity()));
        if (hasError) {
            String instrumentId = safeRead(fieldSet, "INSTRUMENTID");
            String attributeId  = safeRead(fieldSet, "ATTRIBUTEID");
            Integer postingDate = null;
            Integer effectiveDate = null;
            try {
                String pdStr = safeRead(fieldSet, "POSTINGDATE");
                if (pdStr != null && !pdStr.isBlank()) {
                    Date pd = DateUtil.parseDate(pdStr.trim());
                    if (pd != null) postingDate = DateUtil.dateInNumber(pd);
                }
            } catch (Exception ignored) {}
            try {
                String edStr = safeRead(fieldSet, "EFFECTIVEDATE");
                if (edStr != null && !edStr.isBlank()) {
                    Date ed = DateUtil.parseDate(edStr.trim());
                    if (ed != null) effectiveDate = DateUtil.dateInNumber(ed);
                }
            } catch (Exception ignored) {}

            validationLogService.saveAll(
                    errors,
                    ValidationType.CUSTOM_ACTIVITY,
                    tableDefinition.getTableName(),
                    this.jobId,
                    this.tenantId,
                    new ActivityValidationLogService.RowContext(
                            row, instrumentId, attributeId, postingDate, effectiveDate));
            return null; // silently skip — no Spring Batch rollback
        }

        // 3. Map validated FieldSet → MongoDB Document
        return buildDocument(fieldSet);
    }

    // ------------------------------------------------------------------
    // Document building (preserved from original DynamicDataProcessor)
    // ------------------------------------------------------------------

    private Document buildDocument(FieldSet fieldSet) {
        Document document = new Document();

        for (CustomTableColumn col : tableDefinition.getColumns()) {
            String colName  = col.getColumnName();
            String rawValue = safeRead(fieldSet, colName);

            if (rawValue == null || rawValue.trim().isEmpty()) {
                document.append(colName, null);
                continue;
            }

            Object converted = convertData(rawValue.trim(), col.getDataType().name());

            // Special handling: POSTINGDATE / EFFECTIVEDATE → integer YYYYMMDD
            if (colName.equalsIgnoreCase("POSTINGDATE") || colName.equalsIgnoreCase("EFFECTIVEDATE")) {
                if (converted instanceof String strDate) {
                    try {
                        Date d = DateUtil.parseDate(strDate);
                        converted = DateUtil.convertToIntYYYYMMDDFromJavaDate(d);
                    } catch (Exception ignored) {}
                } else if (converted instanceof java.time.Instant inst) {
                    // Instant is returned when the date parses as MM/dd/yyyy via convertData
                    converted = DateUtil.convertToIntYYYYMMDDFromJavaDate(Date.from(inst));
                } else if (converted instanceof LocalDate ld) {
                    Date d = Date.from(ld.atStartOfDay(ZoneOffset.UTC).toInstant());
                    converted = DateUtil.convertToIntYYYYMMDDFromJavaDate(d);
                } else if (converted instanceof Date d) {
                    converted = DateUtil.convertToIntYYYYMMDDFromJavaDate(d);
                }
            }

            // For every other DATE column (e.g. StartDate, EndDate, …):
            // convert LocalDate → java.util.Date at UTC midnight to avoid the MongoDB driver
            // using the JVM local timezone, which shifts the date by the UTC offset
            // (e.g. +05:30 → "2025-02-27T18:30Z" instead of "2025-02-28T00:00Z").
            if (converted instanceof LocalDate ld) {
                converted = Date.from(ld.atStartOfDay(ZoneOffset.UTC).toInstant());
            }

            document.append(colName, converted);

            // Derive accountingPeriodId from POSTINGDATE
            if (colName.equalsIgnoreCase("POSTINGDATE") && converted instanceof LocalDate ld) {
                document.append("periodId", DateUtil.getAccountingPeriodId(ld));
            }
        }

        document.append("_metadata_version", "1.0");
        return document;
    }

    // ------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------

    private static Object convertData(String value, String dataType) {
        return switch (dataType.toUpperCase()) {
            case "NUMBER" -> {
                if (value.contains(".")) yield Double.parseDouble(value);
                yield Long.parseLong(value);
            }
            case "BOOLEAN" -> Boolean.parseBoolean(value);
            case "DATE" -> {
                // Always yield LocalDate so the POSTINGDATE/EFFECTIVEDATE instanceof chain
                // in buildDocument reliably receives LocalDate → convertToIntYYYYMMDD.
                for (String pattern : new String[]{"M/d/yyyy", "MM/dd/yyyy", "MM/d/yyyy", "M/dd/yyyy"}) {
                    try {
                        yield LocalDate.parse(value, DateTimeFormatter.ofPattern(pattern));
                    } catch (Exception ignored) {}
                }
                // ISO fallback (yyyy-MM-dd)
                yield LocalDate.parse(value, DateTimeFormatter.ISO_DATE);
            }
            default -> value;
        };
    }

    private static String safeRead(FieldSet fieldSet, String name) {
        try { return fieldSet.readString(name); }
        catch (Exception e) {
            try { return fieldSet.readString(name.toUpperCase()); }
            catch (Exception ex) { return null; }
        }
    }
}