package com.reserv.dataloader.validation;

import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.entity.ChartOfAccount;
import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.repository.AccountTypesRepository;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.batch.exception.ItemValidationException.ValidationError;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@StepScope
@Slf4j
public class ChartOfAccountValidator {

    private final AccountTypesRepository accountTypesRepository;
    private final Set<String> validAccountSubtypes = new HashSet<>();
    private final Set<String> seenAccountNumbers = new HashSet<>();
    private final Set<String> seenAccountNames = new HashSet<>();
    private static final Pattern ACCOUNT_NUMBER_PATTERN = Pattern.compile("^[a-zA-Z0-9_\\-\\.]+$");
    private static final Pattern ACCOUNT_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_\\-\\.\\s\\(\\)\\[\\]&',/]+$");

    public ChartOfAccountValidator(AccountTypesRepository accountTypesRepository) {
        this.accountTypesRepository = accountTypesRepository;
    }

    @PostConstruct
    public void init() {
        preloadAccountSubtypes();
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

                // Datatype validation: In this context, if the value is present, we check if it's fundamentally a string.
                // Since FlatFileItemReader gives us strings, we check for basic "invalid" patterns if any were specified.
                // For now, we'll assume any non-null string is a valid datatype unless it's empty and mandatory (not specified for attributes).
                // If specific types were required, they would be checked here.
            }
        }

        if (!errors.isEmpty()) {
            throw new ItemValidationException("Validation failed for ChartOfAccount", errors);
        }
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
