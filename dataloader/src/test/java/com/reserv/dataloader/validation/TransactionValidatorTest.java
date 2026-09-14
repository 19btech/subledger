package com.reserv.dataloader.validation;

import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.service.TransactionService;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class TransactionValidatorTest {

    @Mock
    private TransactionService transactionService;

    private TransactionValidator validator;
    private Set<String> existingNames;

    @BeforeEach
    void setUp() {
        validator = new TransactionValidator(transactionService);
        existingNames = new HashSet<>();
    }

    @Test
    void validate_DoubleSpaceInName_ThrowsErrSpc01() {
        Transactions item = new Transactions();
        item.setName("Invalid  Name");
        item.setIsGL(1);
        item.setIsReplayable(1);
        item.setExclusive(1);

        List<ItemValidationException.ValidationError> errors = validator.validate(item, existingNames);

        assertTrue(errors.stream().anyMatch(e -> "ERROR".equals(e.getSeverity()) && ErrorCode.ERR_SPC_01.getCode().equals(e.getErrorCode())));
    }

    @Test
    void validate_SingleInternalSpaceInName_Passes() {
        Transactions item = new Transactions();
        item.setName("Loan Payment");
        item.setIsGL(1);
        item.setIsReplayable(1);
        item.setExclusive(1);

        List<ItemValidationException.ValidationError> errors = validator.validate(item, existingNames);

        assertTrue(errors.stream().noneMatch(e -> "ERROR".equals(e.getSeverity())));
        assertEquals("Loan Payment", item.getName());
    }

    @Test
    void validate_LeadingTrailingSpace_ThrowsErrSpc02() {
        Transactions item = new Transactions();
        item.setName(" Loan Payment");
        item.setIsGL(1);
        item.setIsReplayable(1);
        item.setExclusive(1);

        List<ItemValidationException.ValidationError> errors = validator.validate(item, existingNames);

        assertTrue(errors.stream().anyMatch(e -> "ERROR".equals(e.getSeverity()) && ErrorCode.ERR_SPC_02.getCode().equals(e.getErrorCode())));
    }

    @Test
    void validate_EmptyExclusive_DefaultsTrueWithWarning() {
        Transactions item = new Transactions();
        item.setName("ValidName");
        item.setIsGL(1);
        item.setIsReplayable(1);
        item.setExclusive(-2); // empty sentinel

        List<ItemValidationException.ValidationError> errors = validator.validate(item, existingNames);

        assertTrue(errors.stream().anyMatch(e -> "WARNING".equals(e.getSeverity()) && ErrorCode.WRN_DEF_01.getCode().equals(e.getErrorCode()) && "EXCLUSIVE".equals(e.getColumn())));
        assertEquals(1, item.getExclusive());
    }

    @Test
    void validate_InvalidExclusive_ThrowsErrBool01() {
        Transactions item = new Transactions();
        item.setName("ValidName");
        item.setIsGL(1);
        item.setIsReplayable(1);
        item.setExclusive(-1); // invalid sentinel

        List<ItemValidationException.ValidationError> errors = validator.validate(item, existingNames);

        assertTrue(errors.stream().anyMatch(e -> "ERROR".equals(e.getSeverity()) && ErrorCode.ERR_BOOL_01.getCode().equals(e.getErrorCode()) && "EXCLUSIVE".equals(e.getColumn())));
    }

    @Test
    void validate_BothGlAndExclusiveFalse_ThrowsWrnLogic01() {
        Transactions item = new Transactions();
        item.setName("ValidName");
        item.setIsGL(0);
        item.setIsReplayable(1);
        item.setExclusive(0);

        List<ItemValidationException.ValidationError> errors = validator.validate(item, existingNames);

        assertTrue(errors.stream().anyMatch(e -> "WARNING".equals(e.getSeverity()) && ErrorCode.WRN_LOGIC_01.getCode().equals(e.getErrorCode())));
    }

    @Test
    void validate_GlFalseReplayableFalseExclusiveTrue_NoWrnLogic01() {
        // Regression guard: under the OLD (buggy) condition this combination — isGL=0,
        // isReplayable=0 — would have incorrectly warned. With exclusive (the real
        // "Reportable" field) explicitly true, no "both false" warning should fire.
        Transactions item = new Transactions();
        item.setName("ValidName");
        item.setIsGL(0);
        item.setIsReplayable(0);
        item.setExclusive(1);

        List<ItemValidationException.ValidationError> errors = validator.validate(item, existingNames);

        assertTrue(errors.stream().noneMatch(e -> ErrorCode.WRN_LOGIC_01.getCode().equals(e.getErrorCode())));
    }

    @Test
    void validate_ReplayableRelabeledMessages_SayReplayableNotReportable() {
        Transactions item = new Transactions();
        item.setName("ValidName");
        item.setIsGL(1);
        item.setExclusive(1);
        item.setIsReplayable(-1); // invalid sentinel

        List<ItemValidationException.ValidationError> errors = validator.validate(item, existingNames);

        assertTrue(errors.stream().anyMatch(e -> "ISREPLAYABLE".equals(e.getColumn())
                && ErrorCode.ERR_BOOL_01.getCode().equals(e.getErrorCode())
                && e.getMessage().toLowerCase().contains("replayable")
                && !e.getMessage().toLowerCase().contains("reportable")));
    }

    @Test
    void validate_DuplicateNameInExistingSet_ThrowsErrDup01() {
        Transactions item = new Transactions();
        item.setName("Existing");
        item.setIsGL(1);
        item.setIsReplayable(1);
        item.setExclusive(1);
        existingNames.add("EXISTING");

        List<ItemValidationException.ValidationError> errors = validator.validate(item, existingNames);

        assertTrue(errors.stream().anyMatch(e -> ErrorCode.ERR_DUP_01.getCode().equals(e.getErrorCode())));
    }
}
