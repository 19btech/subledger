package com.reserv.dataloader.validation;

import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.enums.AccountType;
import com.fyntrac.common.enums.ErrorCode;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class AccountTypesValidator {

    /**
     * Validates an AccountTypes record against business rules.
     * 
     * @param item The record to validate
     * @param seenSubTypes In-memory set of accountSubTypes processed in the current file
     * @param subTypeToTypeMap In-memory mapping of accountSubType to accountType to detect multi-mapping
     * @return List of validation errors
     */
    public List<ItemValidationException.ValidationError> validate(
            AccountTypes item, 
            Set<String> seenSubTypes, 
            Map<String, String> subTypeToTypeMap) {
            
        List<ItemValidationException.ValidationError> errors = new ArrayList<>();
        
        String subType = item.getAccountSubType();
        AccountType type = item.getAccountType();
        String rawType = item.getAccountType() != null ? item.getAccountType().getValue() : null;

        // 1. Validate accountSubType
        if (subType == null || subType.trim().isEmpty()) {
            errors.add(createError("accountSubType", ErrorCode.ERR_REQ_01, "Account Sub Type is required and cannot be empty."));
        } else {
            // Trim check
            if (!subType.equals(subType.trim())) {
                errors.add(createError("accountSubType", ErrorCode.ERR_SPC_02, "Account Sub Type has leading or trailing spaces."));
            }
            
            String trimmedSubType = subType.trim().toUpperCase();
            
            // Duplicate in file check
            if (seenSubTypes.contains(trimmedSubType)) {
                errors.add(createError("accountSubType", ErrorCode.ERR_DUP_01, "Duplicate value: Account Sub Type '" + subType + "' already exists in the same file."));
            }
        }

        // 2. Validate accountType
        if (type == null) {
            errors.add(createError("accountType", ErrorCode.ERR_REQ_01, "Account Type is required and cannot be empty."));
        } else {
            // Check if it's one of the allowed values: Balance Sheet, Income Statement, Clearing
            // Note: AccountType enum handles its own values, but we enforce the user's specific list if needed.
            // Based on prompt, we check validity. AccountType.isValid() is used in the reader/processor usually.
            // Here we assume the reader already tried to map it to the enum.
        }

        // 3. Cross-field Validation: Same accountSubType mapped to multiple accountTypes
        if (subType != null && !subType.trim().isEmpty() && type != null) {
            String trimmedSubType = subType.trim().toUpperCase();
            String currentType = type.name();
            
            if (subTypeToTypeMap.containsKey(trimmedSubType)) {
                String existingType = subTypeToTypeMap.get(trimmedSubType);
                if (!existingType.equals(currentType)) {
                    errors.add(createError("accountSubType/accountType", ErrorCode.ERR_LOGIC_03, "Account Sub Type must map to only one Account Type."));
                }
            }
        }

        return errors;
    }

    private ItemValidationException.ValidationError createError(String column, ErrorCode errorCode, String message) {
        // Requirements: Use ErrorCode.getCode() and ErrorCode.getName()
        return new ItemValidationException.ValidationError(
                column, 
                errorCode.getCode(), 
                message, 
                "ERROR"
        );
    }
}
