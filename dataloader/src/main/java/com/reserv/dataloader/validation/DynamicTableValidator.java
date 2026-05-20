package com.reserv.dataloader.validation;

import com.fyntrac.common.entity.CustomTableColumn;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.enums.CustomTableType;
import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.utils.DateUtil;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Metadata-driven validator for dynamic (customer-configured) table CSV rows.
 *
 * <p>Validation is entirely driven by the {@link CustomTableDefinition} fetched at step
 * startup — no fields are hardcoded. Callers pre-load reference Sets once per step
 * in {@code @BeforeStep} to guarantee O(1) per-row lookups (same as AggregationValidator).
 */
@Component
public class DynamicTableValidator {

    /** Only letters, digits, and underscores — matches the tableName/columnName constraint. */
    private static final Pattern ALPHANUM_UNDERSCORE = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Validates a single CSV row against the runtime table definition and preloaded caches.
     *
     * @param fieldSet             Raw FieldSet row from the CSV reader
     * @param tableDef             Runtime table definition (columns, types, nullable flags)
     * @param seenRowKeys          In-memory duplicate key set (instrumentId:attributeId:postingDate)
     * @param validInstrumentIds   Preloaded instrumentIds from InstrumentAttribute (upper-case)
     * @param validAttributeIds    Preloaded attributeIds from InstrumentAttribute (upper-case)
     */
    public List<ItemValidationException.ValidationError> validate(
            FieldSet fieldSet,
            CustomTableDefinition tableDef,
            Set<String> seenRowKeys,
            Set<String> validInstrumentIds,
            Set<String> validAttributeIds) {

        List<ItemValidationException.ValidationError> errors = new ArrayList<>();

        // --- Structural column-level validations (metadata-driven) ---
        for (CustomTableColumn col : tableDef.getColumns()) {
            String colName  = col.getColumnName();
            String rawValue = safeRead(fieldSet, colName);

            // 1. Null / blank check for non-nullable columns
            if (isBlank(rawValue)) {
                if (!col.getNullable()) {
                    errors.add(err(colName, rawValue,
                            ErrorCode.ERR_REQ_05, "Column '" + colName + "' is required (non-nullable)."));
                }
                continue; // nothing more to validate for blank
            }

            String trimmed = rawValue.trim();

            // 2. Leading/trailing space check
            if (!rawValue.equals(trimmed)) {
                errors.add(err(colName, rawValue,
                        ErrorCode.ERR_SPC_03, "Column '" + colName + "' has leading/trailing spaces."));
            }

            // 3. Column name format check (reuses column definition pattern)
            if (!ALPHANUM_UNDERSCORE.matcher(colName).matches()) {
                errors.add(err(colName, rawValue,
                        ErrorCode.ERR_FMT_02, "Column name '" + colName + "' has invalid format."));
            }

            // 4. Datatype validation against declared type
            validateDataType(colName, trimmed, col.getDataType(), errors);
        }

        // --- Semantic validations for well-known system columns ---
        // REFERENCE tables do not carry postingDate / effectiveDate / instrumentId / attributeId
        boolean isOperational = tableDef.getTableType() != CustomTableType.REFERENCE;

        if (isOperational) {
            String rawPostingDate   = safeRead(fieldSet, "POSTINGDATE");
            String rawEffectiveDate = safeRead(fieldSet, "EFFECTIVEDATE");
            String rawInstrumentId  = safeRead(fieldSet, "INSTRUMENTID");
            String rawAttributeId   = safeRead(fieldSet, "ATTRIBUTEID");

            // 5. postingDate
            if (isBlank(rawPostingDate)) {
                errors.add(err("POSTINGDATE", rawPostingDate,
                        ErrorCode.ERR_REQ_02, "Posting date is required."));
            } else if (!isValidDate(rawPostingDate.trim())) {
                errors.add(err("POSTINGDATE", rawPostingDate,
                        ErrorCode.ERR_FMT_DT, "Invalid posting date format. Expected M/d/yyyy."));
            }

            // 6. effectiveDate
            if (isBlank(rawEffectiveDate)) {
                errors.add(err("EFFECTIVEDATE", rawEffectiveDate,
                        ErrorCode.ERR_REQ_02, "Effective date is required."));
            } else if (!isValidDate(rawEffectiveDate.trim())) {
                errors.add(err("EFFECTIVEDATE", rawEffectiveDate,
                        ErrorCode.ERR_FMT_DT, "Invalid effective date format. Expected M/d/yyyy."));
            }

            // 7. instrumentId — mandatory + no spaces + reference check
            if (isBlank(rawInstrumentId)) {
                errors.add(err("INSTRUMENTID", rawInstrumentId,
                        ErrorCode.ERR_REQ_03, "Instrument ID is required."));
            } else {
                if (hasSpaces(rawInstrumentId)) {
                    errors.add(err("INSTRUMENTID", rawInstrumentId,
                            ErrorCode.ERR_SPC_04, "Instrument ID has leading/trailing spaces."));
                }
                if (!validInstrumentIds.contains(normalizeId(rawInstrumentId.trim()).toUpperCase())) {
                    errors.add(err("INSTRUMENTID", rawInstrumentId,
                            ErrorCode.ERR_REF_03, "Instrument ID not found in InstrumentAttribute history."));
                }
            }

            // 8. attributeId — mandatory + no spaces + reference check
            if (isBlank(rawAttributeId)) {
                errors.add(err("ATTRIBUTEID", rawAttributeId,
                        ErrorCode.ERR_REQ_03, "Attribute ID is required."));
            } else {
                if (hasSpaces(rawAttributeId)) {
                    errors.add(err("ATTRIBUTEID", rawAttributeId,
                            ErrorCode.ERR_SPC_04, "Attribute ID has leading/trailing spaces."));
                }
                if (!validAttributeIds.contains(normalizeId(rawAttributeId.trim()).toUpperCase())) {
                    errors.add(err("ATTRIBUTEID", rawAttributeId,
                            ErrorCode.ERR_REF_03, "Attribute ID not found in InstrumentAttribute history."));
                }
            }

            // 9. Duplicate row detection — a row is a duplicate only when ALL column values match.
            //    Build the key from every column defined in the table, so rows that share
            //    instrumentId/attributeId/postingDate but differ in any other column (e.g. StartDate)
            //    are treated as distinct and are NOT flagged as duplicates.
            String fullRowKey = buildFullRowKey(fieldSet, tableDef);
            if (!seenRowKeys.add(fullRowKey)) {
                errors.add(err("INSTRUMENTID", rawInstrumentId,
                        ErrorCode.ERR_DUP_03, "Duplicate row: all column values are identical to a previous row in this file."));
            }
        }

        return errors;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void validateDataType(String colName, String value,
                                  CustomTableColumn.DataType dataType,
                                  List<ItemValidationException.ValidationError> errors) {
        if (dataType == null) return;
        switch (dataType) {
            case NUMBER -> {
                try { new BigDecimal(value); }
                catch (NumberFormatException e) {
                    errors.add(err(colName, value, ErrorCode.ERR_TYPE_01,
                            "Column '" + colName + "' expects a NUMBER but got: " + value));
                }
            }
            case DATE -> {
                if (!isValidDate(value)) {
                    errors.add(err(colName, value, ErrorCode.ERR_TYPE_01,
                            "Column '" + colName + "' expects a DATE but got: " + value));
                }
            }
            case BOOLEAN -> {
                if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                    errors.add(err(colName, value, ErrorCode.ERR_TYPE_01,
                            "Column '" + colName + "' expects BOOLEAN (true/false) but got: " + value));
                }
            }
            case STRING -> {
                // STRING type — no further format enforcement beyond non-null
            }
        }
    }

    public static String normalizeId(String id) {
        if (id == null) return "";
        String t = id.trim();
        return t.endsWith(".0") ? t.substring(0, t.length() - 2) : t;
    }

    /**
     * Builds a composite key from ALL column values in the row (ordered by displayOrder).
     * Two rows are considered duplicates only when every field value is identical.
     * Null / blank values contribute the empty string so they still participate in the key.
     */
    private static String buildFullRowKey(FieldSet fieldSet, CustomTableDefinition tableDef) {
        StringBuilder sb = new StringBuilder();
        tableDef.getColumns().stream()
                .sorted(java.util.Comparator.comparingInt(col ->
                        col.getDisplayOrder() != null ? col.getDisplayOrder() : 0))
                .forEach(col -> {
                    String val = safeRead(fieldSet, col.getColumnName());
                    sb.append(col.getColumnName().toUpperCase())
                      .append("=")
                      .append(val != null ? val.trim() : "")
                      .append("|");
                });
        return sb.toString();
    }

    private static String safeRead(FieldSet fieldSet, String name) {
        try {
            return fieldSet.readString(name);
        } catch (Exception e) {
            try {
                return fieldSet.readString(name.toUpperCase());
            } catch (Exception ex) {
                return null;
            }
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static boolean hasSpaces(String s) {
        return s != null && !s.equals(s.trim());
    }

    private static boolean isValidDate(String s) {
        if (s == null) return false;
        try { DateUtil.parseDate(s); return true; }
        catch (Exception e) {
            for (String pattern : new String[]{"MM/dd/yyyy", "M/d/yyyy", "M/dd/yyyy"}) {
                try { DateTimeFormatter.ofPattern(pattern).parse(s); return true; }
                catch (Exception ignored) {}
            }
            return false;
        }
    }

    private static ItemValidationException.ValidationError err(
            String column, String value, ErrorCode code, String message) {
        return new ItemValidationException.ValidationError(
                column, value, code.name(), message, "ERROR");
    }
}
