package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.enums.AccountType;
import com.fyntrac.common.repository.AccountTypesRepository;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import com.reserv.dataloader.validation.AccountTypesValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Covers the AccountType upload path actually wired to AccountingRuleController
 * (via UploadServiceFactory -> AccountTypeUploadService -> AccountTypeDataLoadConfig).
 *
 * <p>This used to be a two-line pass-through with no validation, no DB-duplicate
 * check, and no RefDataValidationLog logging at all — every upload (including a
 * repeat upload of the same file) blindly inserted new AccountTypes documents.
 */
@ExtendWith(MockitoExtension.class)
class AccountTypeItemProcessorTest {

    @Mock
    private AccountTypesRepository accountTypesRepository;

    @Mock
    private RefDataValidationLogRepository validationLogRepository;

    private AccountTypesValidator validator;
    private AccountTypeItemProcessor processor;

    @BeforeEach
    void setUp() {
        validator = new AccountTypesValidator();
        processor = new AccountTypeItemProcessor(validator, accountTypesRepository, validationLogRepository);

        JobExecution jobExecution = new JobExecution(301L);
        StepExecution stepExecution = new StepExecution("accountTypeImportStep", jobExecution);
        processor.beforeStep(stepExecution);
    }

    @Test
    void testValidRecordPassesSuccessfully() throws Exception {
        AccountTypes item = new AccountTypes();
        item.setAccountSubType("Accrued Interest");
        item.setAccountType(AccountType.BALANCESHEET);

        AccountTypes result = processor.process(item);

        assertNotNull(result);
    }

    @Test
    void testDuplicateInFileReturnsNull() throws Exception {
        AccountTypes item1 = new AccountTypes();
        item1.setAccountSubType("UniqueSubType");
        item1.setAccountType(AccountType.BALANCESHEET);

        AccountTypes item2 = new AccountTypes();
        item2.setAccountSubType("UniqueSubType"); // Same sub type
        item2.setAccountType(AccountType.BALANCESHEET);

        assertNotNull(processor.process(item1));
        assertNull(processor.process(item2), "Should filter out in-file duplicate");
    }

    @Test
    void testDuplicateAlreadyInDbIsCaughtOnReUpload() throws Exception {
        // Simulate the exact bug scenario: same file uploaded a second time, so the
        // sub type is already persisted in the DB from a prior job run.
        AccountTypes existingRecord = new AccountTypes();
        existingRecord.setAccountSubType("DB_SUBTYPE");
        existingRecord.setAccountType(AccountType.BALANCESHEET);

        when(accountTypesRepository.findAll()).thenReturn(Collections.singletonList(existingRecord));

        org.springframework.batch.core.JobParameters jobParams = new JobParametersBuilder()
                .addString("tenantId", "tenant123")
                .toJobParameters();
        JobExecution jobExecution = new JobExecution(302L, jobParams);
        StepExecution stepExecution = new StepExecution("accountTypeImportStep", jobExecution);
        processor.beforeStep(stepExecution);

        AccountTypes item = new AccountTypes();
        item.setAccountSubType("DB_SUBTYPE"); // Matches preloaded DB record
        item.setAccountType(AccountType.BALANCESHEET);

        AccountTypes result = processor.process(item);

        assertNull(result, "Should filter out item whose sub type already exists in the database");
        verify(validationLogRepository, times(1)).saveAll(anyList());
    }
}
