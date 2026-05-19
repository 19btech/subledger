package com.reserv.dataloader.validation;

import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.entity.ChartOfAccount;
import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.repository.AccountTypesRepository;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ChartOfAccountValidatorTest {

    @Mock
    private AccountTypesRepository accountTypesRepository;

    @InjectMocks
    private ChartOfAccountValidator validator;

    @BeforeEach
    void setUp() {
        // Mock some valid account subtypes
        List<AccountTypes> subtypes = new ArrayList<>();
        AccountTypes type1 = new AccountTypes();
        type1.setAccountSubType("ASSET");
        subtypes.add(type1);
        AccountTypes type2 = new AccountTypes();
        type2.setAccountSubType("LIABILITY");
        subtypes.add(type2);

        when(accountTypesRepository.findAll()).thenReturn(subtypes);
        validator.init();
    }

    @Test
    void testValidRecord() {
        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", "ACC_001");
        rawData.put("ACCOUNTNAME", "Cash at Bank");
        rawData.put("ACCOUNTSUBTYPE", "ASSET");
        rawData.put("Attr1", "Value1");

        ChartOfAccount account = new ChartOfAccount();
        account.setAccountNumber("ACC_001");
        account.setAccountName("Cash at Bank");
        account.setAccountSubtype("ASSET");

        assertDoesNotThrow(() -> validator.validate(account, rawData));
    }

    @Test
    void testNullAccountSubType() {
        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTSUBTYPE", null);

        ChartOfAccount account = new ChartOfAccount();

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(account, rawData));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_REQ_01.getCode())));
    }

    @Test
    void testMissingAccountSubTypeInDb() {
        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTSUBTYPE", "UNKNOWN");

        ChartOfAccount account = new ChartOfAccount();

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(account, rawData));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_REF_02.getCode())));
    }

    @Test
    void testAccountNumberValidation() {
        // Null
        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", null);
        assertThrows(ItemValidationException.class, () -> validator.validate(new ChartOfAccount(), rawData));

        // Trim whitespace
        rawData.put("ACCOUNTNUMBER", " ACC_001 ");
        ItemValidationException ex1 = assertThrows(ItemValidationException.class, () -> validator.validate(new ChartOfAccount(), rawData));
        assertTrue(ex1.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_SPC_02.getCode())));

        // No whitespace
        rawData.put("ACCOUNTNUMBER", "ACC 001");
        ItemValidationException ex2 = assertThrows(ItemValidationException.class, () -> validator.validate(new ChartOfAccount(), rawData));
        assertTrue(ex2.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_SPC_01.getCode())));

        // Alphanum, underscore, hyphens, dots should be allowed
        rawData.put("ACCOUNTNUMBER", "ACC-001");
        rawData.put("ACCOUNTNAME", "Cash Account One");
        rawData.put("ACCOUNTSUBTYPE", "ASSET");
        assertDoesNotThrow(() -> validator.validate(new ChartOfAccount(), rawData));

        rawData.put("ACCOUNTNUMBER", "ACC.001");
        rawData.put("ACCOUNTNAME", "Cash Account Two");
        assertDoesNotThrow(() -> validator.validate(new ChartOfAccount(), rawData));

        // Invalid characters (e.g. #) should fail format validation
        rawData.put("ACCOUNTNUMBER", "ACC#001");
        rawData.put("ACCOUNTNAME", "Cash Account Three");
        ItemValidationException ex3 = assertThrows(ItemValidationException.class, () -> validator.validate(new ChartOfAccount(), rawData));
        assertTrue(ex3.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_FMT_01.getCode())));
    }

    @Test
    void testDuplicateAccountNumber() {
        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", "ACC_001");
        rawData.put("ACCOUNTNAME", "Name1");
        rawData.put("ACCOUNTSUBTYPE", "ASSET");

        ChartOfAccount account = new ChartOfAccount();
        account.setAccountNumber("ACC_001");

        // First one passes
        assertDoesNotThrow(() -> validator.validate(account, rawData));

        // Second one with same number fails
        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(account, rawData));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_DUP_01.getCode())));
    }

    @Test
    void testAttributeWhitespace() {
        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", "ACC_001");
        rawData.put("ACCOUNTNAME", "Name1");
        rawData.put("ACCOUNTSUBTYPE", "ASSET");
        rawData.put("CustomAttr", " ValueWithSpace ");

        ChartOfAccount account = new ChartOfAccount();
        account.setAccountNumber("ACC_001");
        account.setAccountName("Name1");
        account.setAccountSubtype("ASSET");

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(account, rawData));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_SPC_03.getCode())));
    }
}
