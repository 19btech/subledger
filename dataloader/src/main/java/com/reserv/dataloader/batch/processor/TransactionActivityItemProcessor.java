package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.enums.ValidationType;
import com.fyntrac.common.repository.InstrumentAttributeRepository;
import com.fyntrac.common.service.TransactionService;
import com.fyntrac.common.utils.DateUtil;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.service.ActivityValidationLogService;
import com.reserv.dataloader.validation.TransactionActivityValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Spring Batch ItemProcessor for TransactionActivity CSV rows.
 *
 * <p>Validation pattern matches {@link AggregateItemProcessor}:
 * <ol>
 *   <li>Pre-load the (small) transaction-name reference set once per step (@BeforeStep); resolve
 *       the chunk's instrument/attribute IDs against InstrumentAttribute in one query per field
 *       (see {@link ActiveInstrumentAttributeIdLookup}).</li>
 *   <li>Validate each row — collect errors without failing the job.</li>
 *   <li>Persist errors to RefDataValidationLog (tenant-scoped).</li>
 *   <li>Return {@code null} to silently skip invalid rows.</li>
 * </ol>
 */
public class TransactionActivityItemProcessor
        implements ItemProcessor<Map<String, Object>, TransactionActivity>, StepExecutionListener,
                   ItemReadListener<Map<String, Object>> {

    private static final Logger log = LoggerFactory.getLogger(TransactionActivityItemProcessor.class);

    private final TransactionActivityValidator validator;
    private final TransactionService transactionService;
    private final ActiveInstrumentAttributeIdLookup idLookup;
    private final ActivityValidationLogService validationLogService;

    // Step-scoped state
    private Long jobId;
    private String tenantId;
    private final AtomicLong rowCounter = new AtomicLong(0);

    // Preloaded reference set — populated once in @BeforeStep
    private final Set<String> validTransactionNames = new HashSet<>();

    public TransactionActivityItemProcessor(
            TransactionActivityValidator validator,
            TransactionService transactionService,
            InstrumentAttributeRepository instrumentAttributeRepository,
            ActivityValidationLogService validationLogService) {
        this.validator = validator;
        this.transactionService = transactionService;
        this.idLookup = new ActiveInstrumentAttributeIdLookup(instrumentAttributeRepository);
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
        log.info("Initializing TransactionActivityItemProcessor for tenant: {} job: {}", tenantId, jobId);

        idLookup.reset();
        validTransactionNames.clear();

        if (tenantId == null || tenantId.isBlank()) {
            log.error("No tenantId provided — reference preload skipped.");
            return;
        }

        TenantContextHolder.runWithTenant(tenantId, () -> {
            try {
                transactionService.getAll().forEach(tx -> {
                    if (tx.getName() != null) {
                        validTransactionNames.add(tx.getName().trim().toUpperCase());
                    }
                });
                log.info("Preloaded {} valid transaction names.", validTransactionNames.size());
            } catch (Exception e) {
                log.error("Failed to preload transaction names", e);
            }
        });
    }

    // Spring Batch reads the whole chunk before processing its first item, so by the time
    // process() runs, every row of the chunk has been collected here. Registered explicitly as an
    // ItemReadListener in the step config: StepBuilder.listener(Object) would not discover it.
    @Override
    public void afterRead(Map<String, Object> item) {
        idLookup.collect(getRaw(item, "INSTRUMENTID"), getRaw(item, "ATTRIBUTEID", "ATRRIBUTEID"));
    }

    // ------------------------------------------------------------------
    // Processing — matches AggregateItemProcessor.process()
    // ------------------------------------------------------------------

    @Override
    public TransactionActivity process(Map<String, Object> item) throws Exception {

        // 1. Validate inside tenant context
        long row = rowCounter.incrementAndGet();
        String rawInstrumentId = getRaw(item, "INSTRUMENTID");
        String rawAttributeId  = getRaw(item, "ATTRIBUTEID", "ATRRIBUTEID");
        List<ItemValidationException.ValidationError> errors;
        if (tenantId != null) {
            errors = TenantContextHolder.runWithTenant(tenantId, () -> {
                idLookup.ensureChecked(rawInstrumentId, rawAttributeId);
                return validator.validate(item, idLookup.validInstrumentIds(), idLookup.validAttributeIds(),
                        validTransactionNames);
            });
        } else {
            idLookup.ensureChecked(rawInstrumentId, rawAttributeId);
            errors = validator.validate(item, idLookup.validInstrumentIds(), idLookup.validAttributeIds(),
                    validTransactionNames);
        }

        // 2. Persist errors to activity_data_validation_log and skip row
        boolean hasError = errors.stream().anyMatch(e -> "ERROR".equals(e.getSeverity()));
        if (hasError) {
            String instrumentId = toStr(item.get("INSTRUMENTID"));
            String attributeId  = toStr(item.get("ATTRIBUTEID"));
            int postingDate = 0;
            try {
                Date pd = DateUtil.parseDate(toStr(item.get("POSTINGDATE")));
                if (pd != null) postingDate = DateUtil.dateInNumber(pd);
            } catch (Exception ignored) {}
            int effectiveDate = 0;
            try {
                Date ed = parseDate(item.get("TRANSACTIONDATE"));
                if (ed != null) effectiveDate = DateUtil.dateInNumber(ed);
            } catch (Exception ignored) {}

            validationLogService.saveAll(
                    errors,
                    ValidationType.ACTIVITY,
                    "TransactionActivity",
                    this.jobId,
                    this.tenantId,
                    new ActivityValidationLogService.RowContext(
                            row, instrumentId, attributeId, postingDate, effectiveDate));
            return null;
        }

        // 3. Map raw CSV row to entity
        String instrumentId   = "";
        String attributeId    = "";
        String transactionName = "";
        int postingDate  = 0;
        int effectiveDate = 0;
        BigDecimal amount = BigDecimal.ZERO;

        for (Map.Entry<String, Object> entry : item.entrySet()) {
            String key   = entry.getKey();
            Object value = entry.getValue();
            if (key == null || key.isBlank()) continue;

            switch (key.toUpperCase()) {
                case "ACTIVITYUPLOADID" -> { /* skip upload metadata */ }

                case "TRANSACTIONDATE" -> {
                    Date d = parseDate(value);
                    if (d != null) effectiveDate = DateUtil.dateInNumber(d);
                }

                case "INSTRUMENTID" ->
                    instrumentId = TransactionActivityValidator.normalizeId(toStr(value));

                case "ATTRIBUTEID", "ATRRIBUTEID" ->
                    attributeId = toStr(value); // preserve raw value (e.g. "1.0")

                case "POSTINGDATE" -> {
                    try {
                        Date pDate = DateUtil.parseDate(toStr(value));
                        postingDate = DateUtil.dateInNumber(pDate);
                    } catch (Exception e) {
                        log.warn("Could not parse postingDate '{}': {}", value, e.getMessage());
                    }
                }

                case "AMOUNT" -> {
                    try {
                        amount = new BigDecimal(toStr(value).trim());
                    } catch (NumberFormatException e) {
                        log.warn("Could not parse amount '{}': {}", value, e.getMessage());
                    }
                }

                case "TRANSACTIONNAME", "TRANSACTIONTYPE" ->
                    transactionName = toStr(value);
            }
        }

        // 4. Build and return entity via builder (keeps existing builder pattern)
        TransactionActivity ta = TransactionActivity.builder()
                .instrumentId(instrumentId)
                .attributeId(attributeId)
                .transactionName(transactionName)
                .amount(amount.setScale(4, RoundingMode.HALF_UP))
                .postingDate(postingDate)
                .effectiveDate(effectiveDate)
                .source(Source.ETL)
                .build();

        ta.setValidationErrors(new ArrayList<>());
        return ta;
    }

    // ------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------

    private static Date parseDate(Object value) {
        if (value instanceof String s) {
            String t = s.trim();
            for (String pattern : new String[]{"MM/dd/yyyy", "M/d/yyyy", "M/dd/yyyy"}) {
                try {
                    LocalDate ld = LocalDate.parse(t, DateTimeFormatter.ofPattern(pattern));
                    return Date.from(ld.atStartOfDay(ZoneOffset.UTC).toInstant());
                } catch (Exception ignored) {}
            }
            try { return DateUtil.parseDate(t); } catch (Exception ignored) {}
        }
        return null;
    }

    private static String toStr(Object value) {
        return value != null ? String.valueOf(value).trim() : "";
    }

    private static String getRaw(Map<String, Object> item, String... keys) {
        for (String key : keys) {
            for (Map.Entry<String, Object> entry : item.entrySet()) {
                if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(key)) {
                    return entry.getValue() != null ? String.valueOf(entry.getValue()) : null;
                }
            }
        }
        return null;
    }
}
