package com.reserv.dataloader.validation;

import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.repository.AttributesRepository;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates InstrumentAttribute records loaded from CSV.
 *
 * <p>Supports batch-mode validation using a preloaded Set of valid attribute IDs
 * to avoid per-row DB lookups (O(1) access pattern, same as AggregationValidator).</p>
 */
@Component
public class InstrumentAttributeValidator {

    private final AttributesRepository attributesRepository;

    @Autowired
    public InstrumentAttributeValidator(AttributesRepository attributesRepository) {
        this.attributesRepository = attributesRepository;
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Batch-optimised overload: caller pre-loads the valid attributeId set once
     * during {@code @BeforeStep} so this method never hits the DB.
     */
    public List<ItemValidationException.ValidationError> validate(
            Map<String, Object> item,
            Set<String> validAttributeIds) {

        List<ItemValidationException.ValidationError> errors = new ArrayList<>();

        String rawPostingDate   = getRaw(item, "POSTINGDATE");
        String rawEffectiveDate = getRaw(item, "EFFECTIVEDATE");
        String rawInstrumentId  = getRaw(item, "INSTRUMENTID");

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
            errors.add(err("EFFECTIVEDATE", rawEffectiveDate,
                    ErrorCode.ERR_REQ_02, "Effective date is required."));
        } else if (!isValidDate(rawEffectiveDate.trim())) {
            errors.add(err("EFFECTIVEDATE", rawEffectiveDate,
                    ErrorCode.ERR_FMT_DT, "Invalid effective date format. Expected M/d/yyyy."));
        }

        // 3. instrumentId — mandatory + no leading/trailing spaces
        if (isBlank(rawInstrumentId)) {
            errors.add(err("INSTRUMENTID", rawInstrumentId,
                    ErrorCode.ERR_REQ_03, "Instrument ID is required."));
        } else if (hasSpaces(rawInstrumentId)) {
            errors.add(err("INSTRUMENTID", rawInstrumentId,
                    ErrorCode.ERR_SPC_01, "Instrument ID contains leading/trailing spaces."));
        }

        // ATTRIBUTEID and USERID are intentionally not validated.

        return errors;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Strip trailing ".0" that Excel/CSV adds to integer-like numeric values. */
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
            try {
                DateTimeFormatter.ofPattern("MM/dd/yyyy").parse(dateStr);
                return true;
            } catch (Exception ex) {
                try {
                    DateTimeFormatter.ofPattern("M/d/yyyy").parse(dateStr);
                    return true;
                } catch (Exception ex2) {
                    return false;
                }
            }
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
