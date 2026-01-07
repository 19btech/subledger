package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.utils.DateUtil;
import org.bson.types.ObjectId; // Make sure to import this for ID generation
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class InstrumentAttributeItemProcessor implements ItemProcessor<Map<String, Object>, InstrumentAttribute>, StepExecutionListener {

    @Autowired
    private com.fyntrac.common.entity.factory.InstrumentAttributeFactory instrumentAttributeFactory;

    private Long runId;
    private String tenantId;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.runId = stepExecution.getJobParameters().getLong("run.id");
        this.tenantId = stepExecution.getJobParameters().getString("tenantId");
    }

    @Override
    public InstrumentAttribute process(Map<String, Object> item) throws Exception {
        final InstrumentAttribute instrumentAttribute = new InstrumentAttribute();
        final Map<String, Object> attributes = new HashMap<>();
        Date effectiveDate = null;
        String instrumentId = "";
        String attributeId = "";
        int postingDate = 0;

        for (Map.Entry<String, Object> entry : item.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.isBlank()) continue;

            Object value = entry.getValue();

            switch (key.toUpperCase()) {
                case "ACTIVITYUPLOADID":
                    continue;

                case "EFFECTIVEDATE":
                    if (value instanceof String stringVal) {
                        LocalDate localDate = LocalDate.parse(stringVal, DateTimeFormatter.ofPattern("M/dd/yyyy"));
                        effectiveDate = Date.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());
                    }
                    break;

                case "INSTRUMENTID":
                    instrumentId = String.valueOf(value);
                    break;

                case "ATTRIBUTEID":
                    attributeId = String.valueOf(value);
                    break;

                case "POSTINGDATE":
                    if (value instanceof String strDate) {
                        Date pDate = DateUtil.parseDate(strDate);
                        postingDate = DateUtil.dateInNumber(pDate);
                    }
                    break;

                default:
                    attributes.put(key, inferType(value));
                    break;
            }
        }

        InstrumentAttribute result = instrumentAttributeFactory.create(
                this.tenantId,
                instrumentId,
                attributeId,
                effectiveDate,
                0,
                postingDate,
                Source.ETL,
                attributes
        );

        // --- CRITICAL FIX: PRE-GENERATE ID ---
        // This ensures the ID exists before the Writer tries to use it for linking.
        // If your entity uses "VersionId" as the primary key or link, ensure that field is populated.
        // Assuming 'Id' is the MongoDB _id:
        if (result.getId() == null) {
            result.setId(new ObjectId().toString());
        }

        return result;
    }

    private Object inferType(Object value) {
        if (value == null) return null;

        if (value instanceof String strVal) {
            String trimmed = strVal.trim();

            if (trimmed.equalsIgnoreCase("true") || trimmed.equalsIgnoreCase("false")) {
                return Boolean.parseBoolean(trimmed);
            }

            try {
                if (trimmed.contains(".")) {
                    return Double.parseDouble(trimmed);
                } else {
                    return Long.parseLong(trimmed);
                }
            } catch (NumberFormatException ignored) {}

            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
                LocalDate localDate = LocalDate.parse(trimmed, formatter);
                return Date.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());
            } catch (Exception ignored) {}

            return trimmed;
        }

        if (value instanceof Number || value instanceof Boolean || value instanceof Date) {
            return value;
        }

        return value.toString();
    }
}