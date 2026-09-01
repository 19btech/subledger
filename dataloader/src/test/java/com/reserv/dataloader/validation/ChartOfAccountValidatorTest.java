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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ChartOfAccountValidatorTest {

    @Mock
    private AccountTypesRepository accountTypesRepository;

    @Mock
    private AttributesRepository attributesRepository;

    @Mock
    private ChartOfAccountRepository chartOfAccountRepository;

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
        // Not every test cares about attribute datatype metadata; stub leniently so tests that
        // don't touch it aren't forced to also declare this expectation.
        lenient().when(attributesRepository.findAll()).thenReturn(Collections.emptyList());
        // Same for existing ChartOfAccount records — only the DB-duplicate test below cares.
        lenient().when(chartOfAccountRepository.findAll()).thenReturn(Collections.emptyList());
        validator.init();
    }

    private void withAttributeMetadata(String attributeName, DataType dataType) {
        Attributes attribute = new Attributes();
        attribute.setAttributeName(attributeName);
        attribute.setDataType(dataType);
        when(attributesRepository.findAll()).thenReturn(List.of(attribute));
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
    void testDuplicateAlreadyInDbIsCaughtOnReUpload() {
        // Simulate the exact bug scenario: same file uploaded a second time, so the
        // accountNumber/accountName are already persisted in the DB from a prior job run.
        // Without the DB preload, ChartOfAccountValidator only tracked in-file duplicates
        // (its sets start empty on every @StepScope-fresh run), so a repeat upload would
        // silently pass validation and insert a duplicate document.
        ChartOfAccount existing = new ChartOfAccount();
        existing.setAccountNumber("ACC_DB_001");
        existing.setAccountName("Existing Cash Account");
        when(chartOfAccountRepository.findAll()).thenReturn(List.of(existing));
        validator.init();

        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", "ACC_DB_001");
        rawData.put("ACCOUNTNAME", "Existing Cash Account");
        rawData.put("ACCOUNTSUBTYPE", "ASSET");

        ChartOfAccount account = new ChartOfAccount();
        account.setAccountNumber("ACC_DB_001");
        account.setAccountName("Existing Cash Account");

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(account, rawData));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_DUP_01.getCode())),
                "Should flag the row as a duplicate because it already exists in the database");
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

    @Test
    void testNumberAttributeInvalidValue() {
        withAttributeMetadata("Amount", DataType.NUMBER);

        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", "ACC_001");
        rawData.put("ACCOUNTNAME", "Name1");
        rawData.put("ACCOUNTSUBTYPE", "ASSET");
        rawData.put("Amount", "not-a-number");

        ChartOfAccount account = new ChartOfAccount();
        account.setAccountNumber("ACC_001");
        account.setAccountName("Name1");
        account.setAccountSubtype("ASSET");

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(account, rawData));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_TYPE_01.getCode())));
    }

    @Test
    void testNumberAttributeValidValue() {
        withAttributeMetadata("Amount", DataType.NUMBER);

        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", "ACC_001");
        rawData.put("ACCOUNTNAME", "Name1");
        rawData.put("ACCOUNTSUBTYPE", "ASSET");
        rawData.put("Amount", "123.45");

        ChartOfAccount account = new ChartOfAccount();
        account.setAccountNumber("ACC_001");
        account.setAccountName("Name1");
        account.setAccountSubtype("ASSET");

        assertDoesNotThrow(() -> validator.validate(account, rawData));
    }

    @Test
    void testDateAttributeInvalidValue() {
        withAttributeMetadata("OpenDate", DataType.DATE);

        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", "ACC_001");
        rawData.put("ACCOUNTNAME", "Name1");
        rawData.put("ACCOUNTSUBTYPE", "ASSET");
        rawData.put("OpenDate", "not-a-date");

        ChartOfAccount account = new ChartOfAccount();
        account.setAccountNumber("ACC_001");
        account.setAccountName("Name1");
        account.setAccountSubtype("ASSET");

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(account, rawData));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_TYPE_01.getCode())));
    }

    @Test
    void testDateAttributeValidValue() {
        withAttributeMetadata("OpenDate", DataType.DATE);

        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", "ACC_001");
        rawData.put("ACCOUNTNAME", "Name1");
        rawData.put("ACCOUNTSUBTYPE", "ASSET");
        rawData.put("OpenDate", "2024-01-15");

        ChartOfAccount account = new ChartOfAccount();
        account.setAccountNumber("ACC_001");
        account.setAccountName("Name1");
        account.setAccountSubtype("ASSET");

        assertDoesNotThrow(() -> validator.validate(account, rawData));
    }

    @Test
    void testBooleanAttributeInvalidValue() {
        withAttributeMetadata("IsActive", DataType.BOOLEAN);

        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", "ACC_001");
        rawData.put("ACCOUNTNAME", "Name1");
        rawData.put("ACCOUNTSUBTYPE", "ASSET");
        rawData.put("IsActive", "maybe");

        ChartOfAccount account = new ChartOfAccount();
        account.setAccountNumber("ACC_001");
        account.setAccountName("Name1");
        account.setAccountSubtype("ASSET");

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(account, rawData));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_TYPE_01.getCode())));
    }

    @Test
    void testBooleanAttributeValidValueIsCaseInsensitive() {
        withAttributeMetadata("IsActive", DataType.BOOLEAN);

        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", "ACC_001");
        rawData.put("ACCOUNTNAME", "Name1");
        rawData.put("ACCOUNTSUBTYPE", "ASSET");
        rawData.put("IsActive", "FALSE");

        ChartOfAccount account = new ChartOfAccount();
        account.setAccountNumber("ACC_001");
        account.setAccountName("Name1");
        account.setAccountSubtype("ASSET");

        assertDoesNotThrow(() -> validator.validate(account, rawData));
    }

    @Test
    void testAttributeNotInMetadata_SkipsDatatypeCheck() {
        // No metadata mocked for "CustomAttr" -> only the whitespace rule applies, matching
        // pre-existing behavior when AttributesRepository has no entry for a given field.
        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", "ACC_001");
        rawData.put("ACCOUNTNAME", "Name1");
        rawData.put("ACCOUNTSUBTYPE", "ASSET");
        rawData.put("CustomAttr", "anything-goes-here");

        ChartOfAccount account = new ChartOfAccount();
        account.setAccountNumber("ACC_001");
        account.setAccountName("Name1");
        account.setAccountSubtype("ASSET");

        assertDoesNotThrow(() -> validator.validate(account, rawData));
    }
}
