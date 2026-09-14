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
        // The entity is populated from the row exactly as ChartOfAccountItemProcessor does, so
        // each call carries its own composite key and the format checks below are isolated from
        // duplicate detection.

        // Null
        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", null);
        assertThrows(ItemValidationException.class,
                () -> validator.validate(account(null, null, null, Map.of()), rawData));

        // Trim whitespace
        rawData.put("ACCOUNTNUMBER", " ACC_001 ");
        ItemValidationException ex1 = assertThrows(ItemValidationException.class,
                () -> validator.validate(account(" ACC_001 ", null, null, Map.of()), rawData));
        assertTrue(ex1.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_SPC_02.getCode())));

        // No whitespace
        rawData.put("ACCOUNTNUMBER", "ACC 001");
        ItemValidationException ex2 = assertThrows(ItemValidationException.class,
                () -> validator.validate(account("ACC 001", null, null, Map.of()), rawData));
        assertTrue(ex2.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_SPC_01.getCode())));

        // Alphanum, underscore, hyphens, dots should be allowed
        rawData.put("ACCOUNTNUMBER", "ACC-001");
        rawData.put("ACCOUNTNAME", "Cash Account One");
        rawData.put("ACCOUNTSUBTYPE", "ASSET");
        assertDoesNotThrow(() -> validator.validate(
                account("ACC-001", "Cash Account One", "ASSET", Map.of()), rawData));

        rawData.put("ACCOUNTNUMBER", "ACC.001");
        rawData.put("ACCOUNTNAME", "Cash Account Two");
        assertDoesNotThrow(() -> validator.validate(
                account("ACC.001", "Cash Account Two", "ASSET", Map.of()), rawData));

        // Invalid characters (e.g. #) should fail format validation
        rawData.put("ACCOUNTNUMBER", "ACC#001");
        rawData.put("ACCOUNTNAME", "Cash Account Three");
        ItemValidationException ex3 = assertThrows(ItemValidationException.class,
                () -> validator.validate(account("ACC#001", "Cash Account Three", "ASSET", Map.of()), rawData));
        assertTrue(ex3.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_FMT_01.getCode())));
    }

    // ------------------------------------------------------------------
    // Uniqueness is the composite of accountNumber + accountName + accountSubtype TOGETHER
    // WITH every custom attribute value. The triple alone is not unique.
    // ------------------------------------------------------------------

    private static Map<String, Object> rawRow(String number, String name, String subtype) {
        Map<String, Object> rawData = new HashMap<>();
        rawData.put("ACCOUNTNUMBER", number);
        rawData.put("ACCOUNTNAME", name);
        rawData.put("ACCOUNTSUBTYPE", subtype);
        return rawData;
    }

    private static ChartOfAccount account(String number, String name, String subtype, Map<String, Object> attributes) {
        ChartOfAccount account = new ChartOfAccount();
        account.setAccountNumber(number);
        account.setAccountName(name);
        account.setAccountSubtype(subtype);
        account.setAttributes(attributes);
        return account;
    }

    @Test
    void testExactDuplicateIsRejected() {
        Map<String, Object> rawData = rawRow("ACC_001", "Cash", "ASSET");

        // Everything matches, attributes included -> the second row is a duplicate.
        assertDoesNotThrow(() -> validator.validate(
                account("ACC_001", "Cash", "ASSET", Map.of("DEPT", "NY")), rawData));

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(
                account("ACC_001", "Cash", "ASSET", Map.of("DEPT", "NY")), rawData));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_DUP_01.getCode())));
    }

    @Test
    void testSameTripleWithDifferentAttributesIsAllowed() {
        Map<String, Object> rawData = rawRow("ACC_001", "Cash", "ASSET");

        assertDoesNotThrow(() -> validator.validate(
                account("ACC_001", "Cash", "ASSET", Map.of("DEPT", "NY")), rawData));

        // Same number/name/subtype, different attribute value -> a distinct mapping, not a duplicate.
        assertDoesNotThrow(() -> validator.validate(
                account("ACC_001", "Cash", "ASSET", Map.of("DEPT", "LA")), rawData));
    }

    @Test
    void testAccountNumberReusedUnderDifferentNameOrSubtypeIsAllowed() {
        assertDoesNotThrow(() -> validator.validate(
                account("ACC_001", "Cash", "ASSET", Map.of()), rawRow("ACC_001", "Cash", "ASSET")));

        // Same account number, different name.
        assertDoesNotThrow(() -> validator.validate(
                account("ACC_001", "Petty Cash", "ASSET", Map.of()), rawRow("ACC_001", "Petty Cash", "ASSET")));

        // Same account number and name, different subtype.
        assertDoesNotThrow(() -> validator.validate(
                account("ACC_001", "Cash", "LIABILITY", Map.of()), rawRow("ACC_001", "Cash", "LIABILITY")));
    }

    @Test
    void testAccountNameReusedUnderDifferentNumberIsAllowed() {
        assertDoesNotThrow(() -> validator.validate(
                account("ACC_001", "Cash", "ASSET", Map.of()), rawRow("ACC_001", "Cash", "ASSET")));

        // Same name, different account number.
        assertDoesNotThrow(() -> validator.validate(
                account("ACC_002", "Cash", "ASSET", Map.of()), rawRow("ACC_002", "Cash", "ASSET")));
    }

    @Test
    void testAttributeOrderAndCaseDoNotAffectDuplicateDetection() {
        Map<String, Object> rawData = rawRow("ACC_001", "Cash", "ASSET");

        Map<String, Object> first = new LinkedHashMap<>();
        first.put("DEPT", "NY");
        first.put("REGION", "East");

        Map<String, Object> sameButReordered = new LinkedHashMap<>();
        sameButReordered.put("REGION", "east");   // different case
        sameButReordered.put("DEPT", " NY ");     // different surrounding whitespace

        assertDoesNotThrow(() -> validator.validate(account("ACC_001", "Cash", "ASSET", first), rawData));

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(
                account("ACC_001", "Cash", "ASSET", sameButReordered), rawData));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_DUP_01.getCode())),
                "Attribute map order, case and padding must not make an identical record look distinct");
    }

    @Test
    void testDuplicateAlreadyInDbIsCaughtOnReUpload() {
        // Simulate the exact bug scenario: same file uploaded a second time, so the
        // accountNumber/accountName are already persisted in the DB from a prior job run.
        // Without the DB preload, ChartOfAccountValidator only tracked in-file duplicates
        // (its sets start empty on every @StepScope-fresh run), so a repeat upload would
        // silently pass validation and insert a duplicate document.
        ChartOfAccount existing = account("ACC_DB_001", "Existing Cash Account", "ASSET", Map.of("DEPT", "NY"));
        when(chartOfAccountRepository.findAll()).thenReturn(List.of(existing));
        validator.init();

        Map<String, Object> rawData = rawRow("ACC_DB_001", "Existing Cash Account", "ASSET");

        ChartOfAccount account = account("ACC_DB_001", "Existing Cash Account", "ASSET", Map.of("DEPT", "NY"));

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
