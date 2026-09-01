package com.reserv.dataloader.validation;

import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.entity.ChartOfAccount;
import com.fyntrac.common.enums.DataType;
import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.repository.AccountTypesRepository;
import com.fyntrac.common.repository.AttributesRepository;
import com.fyntrac.common.repository.ChartOfAccountRepository;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.batch.exception.ItemValidationException.ValidationError;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@StepScope
@Slf4j
public class ChartOfAccountValidator {

    private final AccountTypesRepository accountTypesRepository;
    private final AttributesRepository attributesRepository;
    private final ChartOfAccountRepository chartOfAccountRepository;
    private final Set<String> validAccountSubtypes = new HashSet<>();
    private final Set<String> seenAccountNumbers = new HashSet<>();
    private final Set<String> seenAccountNames = new HashSet<>();
    private final Map<String, DataType> attributeDataTypeByName = new HashMap<>();
    private static final Pattern ACCOUNT_NUMBER_PATTERN = Pattern.compile("^[a-zA-Z0-9_\\-\\.]+$");
    private static final Pattern ACCOUNT_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_\\-\\.\\s\\(\\)\\[\\]&',/]+$");

    public ChartOfAccountValidator(AccountTypesRepository accountTypesRepository) {
        this(accountTypesRepository, null, null);
    }

    public ChartOfAccountValidator(AccountTypesRepository accountTypesRepository, AttributesRepository attributesRepository) {
        this(accountTypesRepository, attributesRepository, null);
    }

    public ChartOfAccountValidator(AccountTypesRepository accountTypesRepository, AttributesRepository attributesRepository,
                                    ChartOfAccountRepository chartOfAccountRepository) {
        this.accountTypesRepository = accountTypesRepository;
        this.attributesRepository = attributesRepository;
        this.chartOfAccountRepository = chartOfAccountRepository;
    }

    @PostConstruct
    public void init() {
        preloadAccountSubtypes();
        preloadAttributeDataTypes();
        preloadExistingAccounts();
    }

    /**
     * Preloads accountNumber/accountName of every ChartOfAccount record already persisted,
     * so that re-uploading a file whose rows were already loaded in a previous job run is
     * flagged as a duplicate (ERR_DUP_01) instead of being silently re-inserted. Without this,
     * seenAccountNumbers/seenAccountNames only ever caught in-file duplicates (this bean is
     * @StepScope, so a fresh instance — and empty sets — is created for every run/request).
     *
     * <p>Only wired for the batch upload path (see ChartOfAccountDataLoadConfig); the single-record
     * REST controller intentionally omits chartOfAccountRepository here, since it already performs
     * its own self-aware (edit-excluding) duplicate check against the DB after calling validate().
     */
    private void preloadExistingAccounts() {
        if (chartOfAccountRepository == null) {
            log.info("ChartOfAccountRepository not available; skipping DB-level duplicate preload.");
            return;
        }
        try {
            for (com.fyntrac.common.entity.ChartOfAccount existing : chartOfAccountRepository.findAll()) {
                if (existing.getAccountNumber() != null) {
                    seenAccountNumbers.add(existing.getAccountNumber());
                }
                if (existing.getAccountName() != null) {
                    seenAccountNames.add(existing.getAccountName());
                }
            }
            log.info("Preloaded {} existing account numbers and {} existing account names for duplicate validation.",
                    seenAccountNumbers.size(), seenAccountNames.size());
        } catch (Exception e) {
            log.warn("Failed to preload existing ChartOfAccount records; DB-level duplicate detection will be skipped for this run.", e);
        }
    }

    private void preloadAccountSubtypes() {
        log.info("Preloading account subtypes for validation...");
        List<AccountTypes> subtypes = accountTypesRepository.findAll();
        for (AccountTypes subtype : subtypes) {
            if (subtype.getAccountSubType() != null) {
                validAccountSubtypes.add(subtype.getAccountSubType().trim());
            }
        }
        log.info("Preloaded {} account subtypes.", validAccountSubtypes.size());
    }

    private void preloadAttributeDataTypes() {
        if (attributesRepository == null) {
            log.info("AttributesRepository not available; skipping datatype-specific attribute validation.");
            return;
        }
        try {
            for (Attributes attribute : attributesRepository.findAll()) {
                if (attribute.getAttributeName() != null && attribute.getDataType() != null) {
                    attributeDataTypeByName.put(attribute.getAttributeName().trim().toUpperCase(), attribute.getDataType());
                }
            }
            log.info("Preloaded {} attribute data types.", attributeDataTypeByName.size());
        } catch (Exception e) {
            log.warn("Failed to preload attribute datatypes; datatype validation will be skipped for this run.", e);
        }
    }

    public void validate(ChartOfAccount account, Map<String, Object> rawData) {
        List<ValidationError> errors = new ArrayList<>();

        // Validate accountSubType
        String rawSubType = (String) rawData.get("ACCOUNTSUBTYPE");
        if (rawSubType == null || rawSubType.trim().isEmpty()) {
            errors.add(new ValidationError("ACCOUNTSUBTYPE", rawSubType, ErrorCode.ERR_REQ_01.getCode(), ErrorCode.ERR_REQ_01.getName(), "ERROR"));
        } else {
            if (!validAccountSubtypes.contains(rawSubType.trim())) {
                errors.add(new ValidationError("ACCOUNTSUBTYPE", rawSubType, ErrorCode.ERR_REF_02.getCode(), ErrorCode.ERR_REF_02.getName(), "ERROR"));
            }
        }

        // Validate accountNumber
        String rawNum = (String) rawData.get("ACCOUNTNUMBER");
        validateAccountNumber(rawNum, errors, seenAccountNumbers);

        // Validate accountName
        String rawName = (String) rawData.get("ACCOUNTNAME");
        validateAccountName(rawName, errors, seenAccountNames);

        // Validate Attributes
        for (Map.Entry<String, Object> entry : rawData.entrySet()) {
            String key = entry.getKey();
            if (key.equalsIgnoreCase("ACTIVITYUPLOADID") || key.equalsIgnoreCase("ACCOUNTNUMBER") ||
                key.equalsIgnoreCase("ACCOUNTNAME") || key.equalsIgnoreCase("ACCOUNTSUBTYPE")) {
                continue;
            }

            Object value = entry.getValue();
            String valStr = value != null ? value.toString() : null;

            if (valStr != null) {
                if (valStr.strip().length() != valStr.length()) {
                    errors.add(new ValidationError(key, valStr, ErrorCode.ERR_SPC_03.getCode(), ErrorCode.ERR_SPC_03.getName(), "ERROR"));
                }

                // Datatype-specific validation, mirroring the frontend's per-attribute-metadata
                // checks (String/Number/Date/Boolean). Skipped when no metadata is known for this
                // attribute name (e.g. AttributesRepository unavailable, or an ad-hoc/unknown field).
                DataType dataType = attributeDataTypeByName.get(key.trim().toUpperCase());
                if (dataType != null && !valStr.trim().isEmpty()) {
                    String trimmed = valStr.trim();
                    boolean valid;
                    switch (dataType) {
                        case NUMBER:
                            valid = isValidNumber(trimmed);
                            break;
                        case DATE:
                            valid = isValidDate(trimmed);
                            break;
                        case BOOLEAN:
                            valid = trimmed.equalsIgnoreCase("true") || trimmed.equalsIgnoreCase("false");
                            break;
                        case STRING:
                        default:
                            valid = true; // whitespace/charset already checked above
                            break;
                    }
                    if (!valid) {
                        errors.add(new ValidationError(key, valStr, ErrorCode.ERR_TYPE_01.getCode(), ErrorCode.ERR_TYPE_01.getName(), "ERROR"));
                    }
                }
            }
        }

        if (!errors.isEmpty()) {
            throw new ItemValidationException("Validation failed for ChartOfAccount", errors);
        }
    }

    private static boolean isValidNumber(String value) {
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isValidDate(String value) {
        // Lenient about format (ISO, MM/dd/yyyy, M/d/yyyy) to mirror the frontend's use of
        // JS Date.parse(), which accepts a broad range of date string shapes.
        String[] patterns = {"MM/dd/yyyy", "M/d/yyyy", "yyyy-MM-dd", "yyyy/MM/dd"};
        for (String pattern : patterns) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(pattern);
                sdf.setLenient(false);
                sdf.parse(value);
                return true;
            } catch (Exception e) {
                // try next pattern
            }
        }
        return false;
    }

    private void validateAccountNumber(String value, List<ValidationError> errors, Set<String> duplicateSet) {
        if (value == null || value.trim().isEmpty()) {
            errors.add(new ValidationError("ACCOUNTNUMBER", value, ErrorCode.ERR_REQ_01.getCode(), ErrorCode.ERR_REQ_01.getName(), "ERROR"));
            return;
        }

        if (value.strip().length() != value.length()) {
            errors.add(new ValidationError("ACCOUNTNUMBER", value, ErrorCode.ERR_SPC_02.getCode(), ErrorCode.ERR_SPC_02.getName(), "ERROR"));
        }

        if (value.contains(" ")) {
            errors.add(new ValidationError("ACCOUNTNUMBER", value, ErrorCode.ERR_SPC_01.getCode(), ErrorCode.ERR_SPC_01.getName(), "ERROR"));
        }

        if (!ACCOUNT_NUMBER_PATTERN.matcher(value).matches()) {
            errors.add(new ValidationError("ACCOUNTNUMBER", value, ErrorCode.ERR_FMT_01.getCode(), ErrorCode.ERR_FMT_01.getName(), "ERROR"));
        }

        if (!duplicateSet.add(value)) {
            errors.add(new ValidationError("ACCOUNTNUMBER", value, ErrorCode.ERR_DUP_01.getCode(), ErrorCode.ERR_DUP_01.getName(), "ERROR"));
        }
    }

    private void validateAccountName(String value, List<ValidationError> errors, Set<String> duplicateSet) {
        if (value == null || value.trim().isEmpty()) {
            errors.add(new ValidationError("ACCOUNTNAME", value, ErrorCode.ERR_REQ_01.getCode(), ErrorCode.ERR_REQ_01.getName(), "ERROR"));
            return;
        }

        if (value.strip().length() != value.length()) {
            errors.add(new ValidationError("ACCOUNTNAME", value, ErrorCode.ERR_SPC_02.getCode(), ErrorCode.ERR_SPC_02.getName(), "ERROR"));
        }

        if (!ACCOUNT_NAME_PATTERN.matcher(value).matches()) {
            errors.add(new ValidationError("ACCOUNTNAME", value, ErrorCode.ERR_FMT_01.getCode(), ErrorCode.ERR_FMT_01.getName(), "ERROR"));
        }

        if (!duplicateSet.add(value)) {
            errors.add(new ValidationError("ACCOUNTNAME", value, ErrorCode.ERR_DUP_01.getCode(), ErrorCode.ERR_DUP_01.getName(), "ERROR"));
        }
    }
}
