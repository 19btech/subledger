package com.reserv.dataloader.validation;

import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.entity.SubledgerMapping;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class SubledgerMappingValidator {

    // State for composite validations
    private final Map<String, String> transactionToSignMap = new ConcurrentHashMap<>();
    private final Map<String, String> transactionSignToEntryTypeMap = new ConcurrentHashMap<>();
    private final Set<String> transactionSignAccountSubTypeKeys = ConcurrentHashMap.newKeySet();

    public SubledgerMappingValidator() {
    }

    public void validate(SubledgerMapping item, Set<String> validTransactionNames, Set<String> validAccountSubTypes) {
        List<ItemValidationException.ValidationError> errors = new ArrayList<>();

        // 1. transactionName validation
        String txName = item.getTransactionName();
        if (txName == null || txName.trim().isEmpty()) {
            errors.add(new ItemValidationException.ValidationError("transactionName", txName, ErrorCode.ERR_REQ_01.getCode(), ErrorCode.ERR_REQ_01.getName(), "ERROR"));
        } else {
            if (txName.startsWith(" ") || txName.endsWith(" ")) {
                errors.add(new ItemValidationException.ValidationError("transactionName", txName, ErrorCode.ERR_SPC_02.getCode(), ErrorCode.ERR_SPC_02.getName(), "ERROR"));
            }
            boolean txExists = validTransactionNames.stream()
                    .anyMatch(name -> name.trim().equalsIgnoreCase(txName.trim()));
            if (!txExists) {
                errors.add(new ItemValidationException.ValidationError("transactionName", txName, ErrorCode.ERR_REF_02.getCode(), "Transaction name does not exist in transaction configuration.", "ERROR"));
            }
        }

        // 2. sign validation
        com.reserv.dataloader.batch.config.SubledgerMappingDataLoadConfig.RawValidationContext ctx =
                com.reserv.dataloader.batch.config.SubledgerMappingDataLoadConfig.RAW_CONTEXT.get();
        String rawSign = (ctx != null && ctx.rawSign != null) ? ctx.rawSign : (item.getSign() != null ? item.getSign().name() : null);
        if (rawSign == null || rawSign.trim().isEmpty()) {
            errors.add(new ItemValidationException.ValidationError("sign", rawSign, ErrorCode.ERR_REQ_01.getCode(), "Sign is required and cannot be empty.", "ERROR"));
        } else {
            String cleanSign = rawSign.trim().toUpperCase();
            if (!cleanSign.equals("POSITIVE") && !cleanSign.equals("NEGATIVE")) {
                errors.add(new ItemValidationException.ValidationError("sign", rawSign, ErrorCode.ERR_LIST_01.getCode(), "Invalid sign. Allowed values are POSITIVE or NEGATIVE.", "ERROR"));
            } else {
                item.setSign(com.fyntrac.common.enums.Sign.valueOf(cleanSign));
            }
        }

        // 3. entryType validation
        String rawEntryType = (ctx != null && ctx.rawEntryType != null) ? ctx.rawEntryType : (item.getEntryType() != null ? item.getEntryType().name() : null);
        if (rawEntryType == null || rawEntryType.trim().isEmpty()) {
            errors.add(new ItemValidationException.ValidationError("entryType", rawEntryType, ErrorCode.ERR_REQ_01.getCode(), "Entry type is required and cannot be empty.", "ERROR"));
        } else {
            String cleanEntryType = rawEntryType.trim();
            if (!cleanEntryType.equalsIgnoreCase("DEBIT") && !cleanEntryType.equalsIgnoreCase("CREDIT")) {
                errors.add(new ItemValidationException.ValidationError("entryType", rawEntryType, ErrorCode.ERR_LIST_01.getCode(), "Invalid entry type. Allowed values are Debit or Credit.", "ERROR"));
            } else {
                item.setEntryType(com.fyntrac.common.enums.EntryType.valueOf(cleanEntryType.toUpperCase()));
            }
        }

        // 4. accountSubType validation
        String accSubType = item.getAccountSubType();
        if (accSubType == null || accSubType.trim().isEmpty()) {
            errors.add(new ItemValidationException.ValidationError("accountSubType", accSubType, ErrorCode.ERR_REQ_01.getCode(), ErrorCode.ERR_REQ_01.getName(), "ERROR"));
        } else {
            if (accSubType.startsWith(" ") || accSubType.endsWith(" ")) {
                errors.add(new ItemValidationException.ValidationError("accountSubType", accSubType, ErrorCode.ERR_SPC_02.getCode(), ErrorCode.ERR_SPC_02.getName(), "ERROR"));
            }
            boolean accExists = validAccountSubTypes.stream()
                    .anyMatch(subType -> subType.trim().equalsIgnoreCase(accSubType.trim()));
            if (!accExists) {
                errors.add(new ItemValidationException.ValidationError("accountSubType", accSubType, ErrorCode.ERR_REF_02.getCode(), "Account sub type does not exist in account configuration.", "ERROR"));
            }
        }

        // --- Composite Validations ---
        // Only perform composite checks if individual fields are valid to avoid NPEs and confusing error messages
        if (errors.isEmpty() && txName != null && accSubType != null) {
            String signStr = item.getSign().name();
            String entryTypeStr = item.getEntryType().name();
            String upperTxName = txName.trim().toUpperCase();
            String upperAccSubType = accSubType.trim().toUpperCase();

            // Rule 1: transactionName + sign (Ambiguous Sign)
            // Multiple rows for same transactionName with conflicting sign input
            String existingSign = transactionToSignMap.putIfAbsent(upperTxName, signStr);
            if (existingSign != null && !existingSign.equals(signStr)) {
                errors.add(new ItemValidationException.ValidationError("sign", signStr, ErrorCode.ERR_LOGIC_04.getCode(), ErrorCode.ERR_LOGIC_04.getName(), "ERROR"));
            }

            // Rule 3: transactionName + sign + entryType (Entry Type Conflict)
            // Multiple entryTypes for same transactionName + sign
            String txSignKey = upperTxName + "|" + signStr;
            String existingEntryType = transactionSignToEntryTypeMap.putIfAbsent(txSignKey, entryTypeStr);
            if (existingEntryType != null && !existingEntryType.equals(entryTypeStr)) {
                errors.add(new ItemValidationException.ValidationError("entryType", entryTypeStr, ErrorCode.ERR_LOGIC_05.getCode(), ErrorCode.ERR_LOGIC_05.getName(), "ERROR"));
            }

            // Rule 2: transactionName + sign + accountSubType (Duplicate Rule)
            // Duplicate combination in file
            String txSignAccKey = txSignKey + "|" + upperAccSubType;
            if (!transactionSignAccountSubTypeKeys.add(txSignAccKey)) {
                errors.add(new ItemValidationException.ValidationError("composite", txSignAccKey, ErrorCode.ERR_DUP_02.getCode(), ErrorCode.ERR_DUP_02.getName(), "ERROR"));
            }
        }

        if (!errors.isEmpty()) {
            throw new ItemValidationException("Validation failed for record", errors);
        }
    }

    public void clearState() {
        transactionToSignMap.clear();
        transactionSignToEntryTypeMap.clear();
        transactionSignAccountSubTypeKeys.clear();
    }
}
