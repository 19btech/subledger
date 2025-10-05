package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class InstrumentAttributeItemProcessor implements ItemProcessor<Map<String, Object>, InstrumentAttribute> {

    @Autowired
    private com.fyntrac.common.entity.factory.InstrumentAttributeFactory instrumentAttributeFactory;

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

        return instrumentAttributeFactory.create(
                instrumentId,
                attributeId,
                effectiveDate,
                0,
                postingDate,
                Source.ETL,
                attributes
        );
    }

    private Object inferType(Object value) {
        if (value == null) return null;

        if (value instanceof String strVal) {
            String trimmed = strVal.trim();

            // Try boolean
            if (trimmed.equalsIgnoreCase("true") || trimmed.equalsIgnoreCase("false")) {
                return Boolean.parseBoolean(trimmed);
            }

            // Try integer/long/decimal
            try {
                if (trimmed.contains(".")) {
                    return Double.parseDouble(trimmed);
                } else {
                    return Long.parseLong(trimmed);
                }
            } catch (NumberFormatException ignored) {}

            // Try date format
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
                LocalDate localDate = LocalDate.parse(trimmed, formatter);
                return Date.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());
            } catch (Exception ignored) {}

            // Default to string
            return trimmed;
        }

        if (value instanceof Number || value instanceof Boolean || value instanceof Date) {
            return value;
        }

        return value.toString(); // fallback
    }
}
