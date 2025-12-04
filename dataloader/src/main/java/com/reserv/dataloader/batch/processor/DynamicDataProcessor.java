package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.CustomTableColumn;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.NumberUtil;
import org.bson.Document;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.transform.FieldSet;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;

public class DynamicDataProcessor implements ItemProcessor<FieldSet, Document> {

    private final CustomTableDefinition tableDefinition;

    public DynamicDataProcessor(CustomTableDefinition tableDefinition) {
        this.tableDefinition = tableDefinition;
    }

    @Override
    public Document process(FieldSet fieldSet) throws Exception {
        Document document = new Document();

        // Iterate through the columns defined in your CustomTableDefinition
        for (CustomTableColumn col : tableDefinition.getColumns()) {
            String colName = col.getColumnName();
            String rawValue = fieldSet.readString(colName.toUpperCase());

            // Skip processing if value is null/empty and nullable is allowed
            if ((rawValue == null || rawValue.trim().isEmpty())) {
                if (!col.getNullable()) {
                    throw new IllegalArgumentException("Column " + colName + " cannot be null");
                }
                document.append(colName, null);
                continue;
            }

            // Type Conversion Logic
            Object convertedValue = convertData(rawValue, col.getDataType().name());

            document.append(colName, convertedValue);

            if(colName.equalsIgnoreCase("PostingDate") && convertedValue instanceof LocalDate) {

                Integer accountingPeriodId = DateUtil.getAccountingPeriodId((LocalDate) convertedValue);
                document.append("periodId", accountingPeriodId);
            }
        }

        // Add metadata if needed (e.g. created_at)
        document.append("_metadata_version", "1.0");

        return document;
    }

    private Object convertData(String value, String dataType) {
        switch (dataType.toUpperCase()) {
            case "NUMBER":
                if (value.contains(".")) {
                    return Double.parseDouble(value);
                }
                return Long.parseLong(value);
            case "BOOLEAN":
                return Boolean.parseBoolean(value);
            case "DATE":
                // FIX: Handle "MM/dd/yyyy" format (e.g., 01/31/2025)
                try {
                    return LocalDate.parse(value, DateTimeFormatter.ofPattern("MM/dd/yyyy"));
                } catch (Exception e) {
                    // Fallback to ISO format (YYYY-MM-DD) if parsing fails
                    return LocalDate.parse(value, DateTimeFormatter.ISO_DATE);
                }
            case "STRING":
            default:
                return value;
        }
    }
}