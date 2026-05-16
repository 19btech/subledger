package com.reserv.dataloader.validation;

import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.enums.AccountType;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AccountTypesValidatorTest {

    private AccountTypesValidator validator;
    private Set<String> seenSubTypes;
    private Map<String, String> subTypeToTypeMap;

    @BeforeEach
    void setUp() {
        validator = new AccountTypesValidator();
        seenSubTypes = new HashSet<>();
        subTypeToTypeMap = new HashMap<>();
    }

    @Test
    void testValidRecord() {
        AccountTypes item = AccountTypes.builder()
                .accountSubType("Cash")
                .accountType(AccountType.BALANCESHEET)
                .build();

        List<ItemValidationException.ValidationError> errors = validator.validate(item, seenSubTypes, subTypeToTypeMap);
        assertTrue(errors.isEmpty(), "Valid record should have no errors");
    }

    @Test
    void testAccountSubTypeNull() {
        AccountTypes item = AccountTypes.builder()
                .accountSubType(null)
                .accountType(AccountType.BALANCESHEET)
                .build();

        List<ItemValidationException.ValidationError> errors = validator.validate(item, seenSubTypes, subTypeToTypeMap);
        assertFalse(errors.isEmpty());
        assertEquals("ERR_REQ_01", errors.get(0).getErrorCode());
        assertEquals("accountSubType", errors.get(0).getColumn());
    }

    @Test
    void testAccountSubTypeTrailingSpaces() {
        AccountTypes item = AccountTypes.builder()
                .accountSubType("Cash ")
                .accountType(AccountType.BALANCESHEET)
                .build();

        List<ItemValidationException.ValidationError> errors = validator.validate(item, seenSubTypes, subTypeToTypeMap);
        assertFalse(errors.isEmpty());
        assertEquals("ERR_SPC_02", errors.get(0).getErrorCode());
    }

    @Test
    void testDuplicateAccountSubType() {
        seenSubTypes.add("CASH");
        AccountTypes item = AccountTypes.builder()
                .accountSubType("Cash")
                .accountType(AccountType.BALANCESHEET)
                .build();

        List<ItemValidationException.ValidationError> errors = validator.validate(item, seenSubTypes, subTypeToTypeMap);
        assertFalse(errors.isEmpty());
        assertEquals("ERR_DUP_01", errors.get(0).getErrorCode());
    }

    @Test
    void testAccountTypeNull() {
        AccountTypes item = AccountTypes.builder()
                .accountSubType("Cash")
                .accountType(null)
                .build();

        List<ItemValidationException.ValidationError> errors = validator.validate(item, seenSubTypes, subTypeToTypeMap);
        assertFalse(errors.isEmpty());
        assertEquals("ERR_REQ_01", errors.get(0).getErrorCode());
        assertEquals("accountType", errors.get(0).getColumn());
    }

    @Test
    void testMultiMappingError() {
        subTypeToTypeMap.put("CASH", AccountType.BALANCESHEET.name());
        
        AccountTypes item = AccountTypes.builder()
                .accountSubType("Cash")
                .accountType(AccountType.INCOMESTATEMENT)
                .build();

        List<ItemValidationException.ValidationError> errors = validator.validate(item, seenSubTypes, subTypeToTypeMap);
        assertFalse(errors.isEmpty());
        assertEquals("ERR_LOGIC_03", errors.get(0).getErrorCode());
    }
}
