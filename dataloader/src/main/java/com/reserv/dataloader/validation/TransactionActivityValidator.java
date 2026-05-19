package com.reserv.dataloader.validation;

import com.fyntrac.common.enums.ErrorCode;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates TransactionActivity CSV records.
 *
 * <p>Batch-optimised: callers pre-load reference Sets once per step (@BeforeStep)
 * so each call to {@link #validate} performs only O(1) lookups — no per-row DB hits.
 */
@Component
public class TransactionActivityValidator {

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * @param item                  Raw CSV row (key = column name, value = cell value)
     * @param validInstrumentIds    Preloaded set of known instrumentIds (upper-case)
     * @param validAttributeIds     Preloaded set of known attributeIds (upper-case)
     * @param validTransactionNames Preloaded set of known transaction names (upper-case)
     */
    public List<ItemValidationException.ValidationError> validate(
            Map<String, Object> item,
            Set<String> validInstrumentIds,
            Set<String> validAttributeIds,
            Set<String> validTransactionNames) {

        List<ItemValidationException.ValidationError> errors = new ArrayList<>();

        String rawPostingDate    = getRaw(item, "POSTINGDATE");
        String rawEffectiveDate  = getRaw(item, "TRANSACTIONDATE");
        String rawInstrumentId   = getRaw(item, "INSTRUMENTID");
        String rawAttributeId    = getRaw(item, "ATTRIBUTEID", "ATRRIBUTEID");
        String rawTransactionName = getRaw(item, "TRANSACTIONNAME", "TRANSACTIONTYPE");
        String rawAmount         = getRaw(item, "AMOUNT");

        // 1. postingDate — mandatory + valid date format
        if (isBlank(rawPostingDate)) {
            errors.add(err("POSTINGDATE", rawPostingDate,
                    ErrorCode.ERR_REQ_02, "Posting date is required."));
        } else if (!isValidDate(rawPostingDate.trim())) {
            errors.add(err("POSTINGDATE", rawPostingDate,
                    ErrorCode.ERR_FMT_DT, "Invalid posting date format. Expected M/d/yyyy."));
        }

        // 2. effectiveDate — mandatory + valid date format
        if (isBlank(rawEffectiveDate)) {
            errors.add(err("TRANSACTIONDATE", rawEffectiveDate,
                    ErrorCode.ERR_REQ_02, "Transaction/effective date is required."));
        } else if (!isValidDate(rawEffectiveDate.trim())) {
            errors.add(err("TRANSACTIONDATE", rawEffectiveDate,
                    ErrorCode.ERR_FMT_DT, "Invalid transaction date format. Expected M/d/yyyy."));
        }

        // 3. instrumentId — mandatory + no spaces + reference check
        if (isBlank(rawInstrumentId)) {
            errors.add(err("INSTRUMENTID", rawInstrumentId,
                    ErrorCode.ERR_REQ_03, "Instrument ID is required."));
        } else {
            if (hasSpaces(rawInstrumentId)) {
                errors.add(err("INSTRUMENTID", rawInstrumentId,
                        ErrorCode.ERR_SPC_01, "Instrument ID contains leading/trailing spaces."));
            }
            String normId = normalizeId(rawInstrumentId.trim());
            if (!validInstrumentIds.contains(normId.toUpperCase())) {
                errors.add(err("INSTRUMENTID", rawInstrumentId,
                        ErrorCode.ERR_REF_08, "Instrument ID not found in InstrumentAttribute table."));
            }
        }

        // 4. attributeId — mandatory + no spaces + reference check
        if (isBlank(rawAttributeId)) {
            errors.add(err("ATTRIBUTEID", rawAttributeId,
                    ErrorCode.ERR_REQ_03, "Attribute ID is required."));
        } else {
            if (hasSpaces(rawAttributeId)) {
                errors.add(err("ATTRIBUTEID", rawAttributeId,
                        ErrorCode.ERR_SPC_01, "Attribute ID contains leading/trailing spaces."));
            }
            String normAttr = normalizeId(rawAttributeId.trim());
            if (!validAttributeIds.contains(normAttr.toUpperCase())) {
                errors.add(err("ATTRIBUTEID", rawAttributeId,
                        ErrorCode.ERR_REF_09, "Attribute ID not found in InstrumentAttribute table."));
            }
        }

        // 5. transactionName — mandatory + reference check
        if (isBlank(rawTransactionName)) {
            errors.add(err("TRANSACTIONNAME", rawTransactionName,
                    ErrorCode.ERR_REQ_01, "Transaction name is required."));
        } else {
            String normTx = rawTransactionName.trim().toUpperCase();
            if (!validTransactionNames.contains(normTx)) {
                errors.add(err("TRANSACTIONNAME", rawTransactionName,
                        ErrorCode.ERR_REF_01, "Transaction name does not exist in transaction config."));
            }
        }

        // 6. amount — mandatory + valid decimal format
        if (isBlank(rawAmount)) {
            errors.add(err("AMOUNT", rawAmount,
                    ErrorCode.ERR_REQ_01, "Amount is required."));
        } else if (!isValidDecimal(rawAmount.trim())) {
            errors.add(err("AMOUNT", rawAmount,
                    ErrorCode.ERR_FMT_DEC, "Invalid decimal format for amount."));
        }

        return errors;
    }

    // -----------------------------------------------------------------------
    // Static helpers (reused by processor)
    // -----------------------------------------------------------------------

    /** Strip trailing ".0" suffix added by Excel/CSV for integer-like numeric values. */
    public static String normalizeId(String id) {
        if (id == null) return "";
        String t = id.trim();
        return t.endsWith(".0") ? t.substring(0, t.length() - 2) : t;
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static boolean hasSpaces(String s) {
        return s != null && !s.equals(s.trim());
    }

    private static boolean isValidDate(String dateStr) {
        if (dateStr == null) return false;
        try {
            DateUtil.parseDate(dateStr);
            return true;
        } catch (Exception e) {
            for (String pattern : new String[]{"MM/dd/yyyy", "M/d/yyyy", "M/dd/yyyy"}) {
                try {
                    DateTimeFormatter.ofPattern(pattern).parse(dateStr);
                    return true;
                } catch (Exception ignored) {}
            }
            return false;
        }
    }

    private static boolean isValidDecimal(String s) {
        if (s == null) return false;
        try {
            new BigDecimal(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private String getRaw(Map<String, Object> item, String... keys) {
        for (String key : keys) {
            for (Map.Entry<String, Object> entry : item.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(key)) {
                    Object val = entry.getValue();
                    return val != null ? String.valueOf(val) : null;
                }
            }
        }
        return null;
    }

    private static ItemValidationException.ValidationError err(
            String column, String value, ErrorCode code, String message) {
        return new ItemValidationException.ValidationError(
                column, value, code.name(), message, "ERROR");
    }
}
