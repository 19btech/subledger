package com.reserv.dataloader.service.model;

import com.fyntrac.common.entity.EventConfiguration;
import com.fyntrac.common.entity.Option;
import com.fyntrac.common.entity.SourceMapping;
import org.apache.poi.ss.usermodel.*;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Component
public class EventConfigurationValidator {

    public ValidationResult validateWorkbookAgainstConfigurations(Workbook workbook,
                                                                  List<EventConfiguration> configurations) {
        ValidationResult result = new ValidationResult();

        if (workbook == null) {
            result.addError("Workbook cannot be null");
            return result;
        }

        if (configurations == null || configurations.isEmpty()) {
            result.addError("Event configurations cannot be null or empty");
            return result;
        }

        // Validate each event configuration
        for (EventConfiguration config : configurations) {
            if (Boolean.TRUE.equals(config.getIsActive()) && !Boolean.TRUE.equals(config.getIsDeleted())) {
                validateEventConfiguration(workbook, config, result);
            }
        }

        return result;
    }

    private void validateEventConfiguration(Workbook workbook, EventConfiguration config, ValidationResult result) {
        String eventId = config.getEventId();

        // 1. Check if sheet exists with same name as eventId (ignore case)
        Sheet sheet = findSheetIgnoreCase(workbook, eventId);
        if (sheet == null) {
            result.addError("Sheet with name '" + eventId + "' not found in workbook");
            return;
        }

        // 2. Get first row as columns and validate required columns
        Row headerRow = sheet.getRow(0);
        if (headerRow == null) {
            result.addError("No header row found in sheet '" + eventId + "'");
            return;
        }

        List<String> columns = extractColumns(headerRow);
        validateRequiredColumns(columns, config.getEventId(), result);

        // 3. Validate source mapping columns
        if (config.getSourceMappings() != null) {
            for (SourceMapping sourceMapping : config.getSourceMappings()) {
                validateSourceMappingColumns(columns, sourceMapping, config.getEventId(), result);
            }
        }
    }

    private Sheet findSheetIgnoreCase(Workbook workbook, String sheetName) {
        for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
            Sheet sheet = workbook.getSheetAt(i);
            if (sheet.getSheetName().equalsIgnoreCase(sheetName)) {
                return sheet;
            }
        }
        return null;
    }

    private List<String> extractColumns(Row headerRow) {
        List<String> columns = new ArrayList<>();
        for (Cell cell : headerRow) {
            String cellValue = getCellValueAsString(cell).trim();
            if (!cellValue.isEmpty()) {
                columns.add(cellValue);
            }
        }
        return columns;
    }

    private String getCellValueAsString(Cell cell) {
        if (cell == null) return "";

        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                } else {
                    return String.valueOf(cell.getNumericCellValue());
                }
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                return cell.getCellFormula();
            default:
                return "";
        }
    }

    private void validateRequiredColumns(List<String> columns, String eventId, ValidationResult result) {
        List<String> requiredColumns = Arrays.asList("PostingDate", "EffectiveDate", "InstrumentId", "AttributeId");
        List<String> missingColumns = new ArrayList<>();

        for (String requiredColumn : requiredColumns) {
            boolean found = columns.stream()
                    .anyMatch(column -> column.equalsIgnoreCase(requiredColumn));
            if (!found) {
                missingColumns.add(requiredColumn);
            }
        }

        if (!missingColumns.isEmpty()) {
            result.addError("Event '" + eventId + "' missing required columns: " + missingColumns);
        }
    }

    private void validateSourceMappingColumns(List<String> columns, SourceMapping sourceMapping,
                                              String eventId, ValidationResult result) {
        String sourceTable = sourceMapping.getSourceTable().getDisplayName();

        if ("ATTRIBUTE".equalsIgnoreCase(sourceTable)) {
            validateAttributeColumns(columns, sourceMapping, eventId, result);
        } else if ("TRANSACTIONS".equalsIgnoreCase(sourceTable) || "BALANCES".equalsIgnoreCase(sourceTable)) {
            validateTransactionOrBalanceColumns(columns, sourceMapping, eventId, result);
        }
    }

    private void validateAttributeColumns(List<String> columns, SourceMapping sourceMapping,
                                          String eventId, ValidationResult result) {
        List<String> missingColumns = new ArrayList<>();
        String sourceTable = sourceMapping.getSourceTable().getDisplayName();
        // For each source column and version type combination
        for (Option sourceColumn : sourceMapping.getSourceColumns()) {
            for (Option versionType : sourceMapping.getVersionType()) {
                String expectedColumn = buildAttributeColumnName(sourceTable, sourceColumn.getValue(), versionType.getValue());
                boolean found = columns.stream()
                        .anyMatch(column -> column.equalsIgnoreCase(expectedColumn));

                if (!found) {
                    missingColumns.add(expectedColumn);
                }
            }
        }

        if (!missingColumns.isEmpty()) {
            result.addError("Event '" + eventId + "' missing attribute columns: " + missingColumns);
        }
    }

    private void validateTransactionOrBalanceColumns(List<String> columns, SourceMapping sourceMapping,
                                                     String eventId, ValidationResult result) {
        List<String> missingColumns = new ArrayList<>();
        String sourceTable = sourceMapping.getSourceTable().getDisplayName();
        // For each source column and data mapping combination
        for (Option sourceColumn : sourceMapping.getSourceColumns()) {
            for (Option dataMapping : sourceMapping.getDataMapping()) {
                String expectedColumn = buildTransactionColumnName(sourceTable, sourceColumn.getValue(), dataMapping.getValue());
                boolean found = columns.stream()
                        .anyMatch(column -> column.equalsIgnoreCase(expectedColumn));

                if (!found) {
                    missingColumns.add(expectedColumn);
                }
            }
        }

        if (!missingColumns.isEmpty()) {
            result.addError("Event '" + eventId + "' missing transaction/balance columns: " + missingColumns);
        }
    }

    private String buildAttributeColumnName(String sourceTable, String sourceColumn, String versionType) {
        return String.format("%s_%s_%s",
                sourceTable.toUpperCase(),
                sourceColumn.toUpperCase(),
                versionType.toUpperCase());
    }

    private String buildTransactionColumnName(String sourceTable, String sourceColumn, String dataMapping) {
        return String.format("%s_%s_%s",
                sourceTable.toUpperCase(),
                sourceColumn.toUpperCase(),
                dataMapping.toUpperCase());
    }

    // Validation Result class
    public static class ValidationResult {
        private List<String> errors = new ArrayList<>();
        private List<String> warnings = new ArrayList<>();

        public void addError(String error) {
            errors.add(error);
        }

        public void addWarning(String warning) {
            warnings.add(warning);
        }

        public boolean isValid() {
            return errors.isEmpty();
        }

        public List<String> getErrors() {
            return Collections.unmodifiableList(errors);
        }

        public List<String> getWarnings() {
            return Collections.unmodifiableList(warnings);
        }

        public String getSummary() {
            if (isValid()) {
                return "Validation passed successfully";
            } else {
                return String.format("Validation failed with %d error(s): %s",
                        errors.size(), String.join("; ", errors));
            }
        }
    }
}