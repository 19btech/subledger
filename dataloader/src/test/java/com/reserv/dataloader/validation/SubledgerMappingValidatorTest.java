package com.reserv.dataloader.validation;

import com.fyntrac.common.entity.SubledgerMapping;
import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.enums.EntryType;
import com.fyntrac.common.enums.Sign;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SubledgerMappingValidatorTest {

    private SubledgerMappingValidator validator;
    private Set<String> validTxNames;
    private Set<String> validAccSubTypes;

    @BeforeEach
    void setUp() {
        validTxNames = new HashSet<>();
        validTxNames.add("TX1");
        validTxNames.add("TX2");

        validAccSubTypes = new HashSet<>();
        validAccSubTypes.add("ACC1");
        validAccSubTypes.add("ACC2");

        validator = new SubledgerMappingValidator();
    }

    @Test
    void validate_ValidRecord_Passes() {
        SubledgerMapping item = new SubledgerMapping();
        item.setTransactionName("TX1");
        item.setSign(Sign.POSITIVE);
        item.setEntryType(EntryType.DEBIT);
        item.setAccountSubType("ACC1");

        assertDoesNotThrow(() -> validator.validate(item, validTxNames, validAccSubTypes));
    }

    @Test
    void validate_NullTransactionName_ThrowsMandatoryField() {
        SubledgerMapping item = new SubledgerMapping();
        item.setTransactionName(null);
        item.setSign(Sign.POSITIVE);
        item.setEntryType(EntryType.DEBIT);
        item.setAccountSubType("ACC1");

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(item, validTxNames, validAccSubTypes));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_REQ_01.getCode())));
    }

    @Test
    void validate_TransactionNameWithSpaces_ThrowsTrimWhitespace() {
        SubledgerMapping item = new SubledgerMapping();
        item.setTransactionName(" TX1 ");
        item.setSign(Sign.POSITIVE);
        item.setEntryType(EntryType.DEBIT);
        item.setAccountSubType("ACC1");

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(item, validTxNames, validAccSubTypes));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_SPC_02.getCode())));
    }

    @Test
    void validate_TransactionNameNotFound_ThrowsRefConfigMissing() {
        SubledgerMapping item = new SubledgerMapping();
        item.setTransactionName("UNKNOWN");
        item.setSign(Sign.POSITIVE);
        item.setEntryType(EntryType.DEBIT);
        item.setAccountSubType("ACC1");

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(item, validTxNames, validAccSubTypes));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_REF_02.getCode())));
    }

    @Test
    void validate_NullSign_ThrowsMandatoryField() {
        SubledgerMapping item = new SubledgerMapping();
        item.setTransactionName("TX1");
        item.setSign(null);
        item.setEntryType(EntryType.DEBIT);
        item.setAccountSubType("ACC1");

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(item, validTxNames, validAccSubTypes));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_REQ_01.getCode())));
    }

    @Test
    void validate_NullEntryType_ThrowsMandatoryField() {
        SubledgerMapping item = new SubledgerMapping();
        item.setTransactionName("TX1");
        item.setSign(Sign.POSITIVE);
        item.setEntryType(null);
        item.setAccountSubType("ACC1");

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(item, validTxNames, validAccSubTypes));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_REQ_01.getCode())));
    }

    @Test
    void validate_NullAccountSubType_ThrowsMandatoryField() {
        SubledgerMapping item = new SubledgerMapping();
        item.setTransactionName("TX1");
        item.setSign(Sign.POSITIVE);
        item.setEntryType(EntryType.DEBIT);
        item.setAccountSubType(null);

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(item, validTxNames, validAccSubTypes));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_REQ_01.getCode())));
    }

    @Test
    void validate_AccountSubTypeWithSpaces_ThrowsTrimWhitespace() {
        SubledgerMapping item = new SubledgerMapping();
        item.setTransactionName("TX1");
        item.setSign(Sign.POSITIVE);
        item.setEntryType(EntryType.DEBIT);
        item.setAccountSubType(" ACC1 ");

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(item, validTxNames, validAccSubTypes));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_SPC_02.getCode())));
    }

    @Test
    void validate_AccountSubTypeNotFound_ThrowsRefConfigMissing() {
        SubledgerMapping item = new SubledgerMapping();
        item.setTransactionName("TX1");
        item.setSign(Sign.POSITIVE);
        item.setEntryType(EntryType.DEBIT);
        item.setAccountSubType("UNKNOWN");

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(item, validTxNames, validAccSubTypes));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_REF_02.getCode())));
    }

    @Test
    void validate_DuplicateRule_ThrowsDuplicateRule() {
        SubledgerMapping item = new SubledgerMapping();
        item.setTransactionName("TX1");
        item.setSign(Sign.POSITIVE);
        item.setEntryType(EntryType.DEBIT);
        item.setAccountSubType("ACC1");

        validator.validate(item, validTxNames, validAccSubTypes);

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(item, validTxNames, validAccSubTypes));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_DUP_02.getCode())));
    }

    @Test
    void validate_AmbiguousSign_ThrowsLogicAmbiguous() {
        SubledgerMapping item1 = new SubledgerMapping();
        item1.setTransactionName("TX1");
        item1.setSign(Sign.POSITIVE);
        item1.setEntryType(EntryType.DEBIT);
        item1.setAccountSubType("ACC1");

        SubledgerMapping item2 = new SubledgerMapping();
        item2.setTransactionName("TX1");
        item2.setSign(Sign.NEGATIVE);
        item2.setEntryType(EntryType.CREDIT);
        item2.setAccountSubType("ACC2");

        validator.validate(item1, validTxNames, validAccSubTypes);

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(item2, validTxNames, validAccSubTypes));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_LOGIC_04.getCode())));
    }

    @Test
    void validate_DebitCreditShareSameSubtype_ThrowsLogicSubtypeClash() {
        SubledgerMapping item1 = new SubledgerMapping();
        item1.setTransactionName("TX1");
        item1.setSign(Sign.POSITIVE);
        item1.setEntryType(EntryType.DEBIT);
        item1.setAccountSubType("ACC1");

        SubledgerMapping item2 = new SubledgerMapping();
        item2.setTransactionName("TX1");
        item2.setSign(Sign.POSITIVE);
        item2.setEntryType(EntryType.CREDIT);
        item2.setAccountSubType("ACC1"); // same subtype as item1's Debit entry

        validator.validate(item1, validTxNames, validAccSubTypes);

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> validator.validate(item2, validTxNames, validAccSubTypes));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> e.getErrorCode().equals(ErrorCode.ERR_LOGIC_06.getCode())));
    }

    @Test
    void validate_DebitCreditPairWithDifferentSubtypes_BothPass() {
        // This is the normal, expected shape of a subledger mapping: the same
        // transactionName+sign has one DEBIT leg and one CREDIT leg, posting to two
        // different account subtypes (e.g. Debit Principal / Credit Cash Receivable).
        // A prior version of this rule rejected every such pair (see the removed
        // ERR_LOGIC_05 "Entry Type Conflict" check) — this asserts that regression stays fixed.
        SubledgerMapping item1 = new SubledgerMapping();
        item1.setTransactionName("TX1");
        item1.setSign(Sign.POSITIVE);
        item1.setEntryType(EntryType.DEBIT);
        item1.setAccountSubType("ACC1");

        SubledgerMapping item2 = new SubledgerMapping();
        item2.setTransactionName("TX1");
        item2.setSign(Sign.POSITIVE);
        item2.setEntryType(EntryType.CREDIT);
        item2.setAccountSubType("ACC2");

        assertDoesNotThrow(() -> validator.validate(item1, validTxNames, validAccSubTypes));
        assertDoesNotThrow(() -> validator.validate(item2, validTxNames, validAccSubTypes));
    }
}
