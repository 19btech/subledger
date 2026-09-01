package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.RefDataValidationLog;
import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TransactionsItemProcessorTest {

    @Mock
    private com.fyntrac.common.service.TransactionService transactionService;

    @Mock
    private com.fyntrac.common.repository.RefDataValidationLogRepository validationLogRepository;

    @Mock
    private com.fyntrac.common.repository.MemcachedRepository memcachedRepository;

    @Mock
    private com.fyntrac.common.repository.TransactionsRepository transactionsRepository;

    private com.reserv.dataloader.validation.TransactionValidator validator;
    private TransactionsItemProcessor processor;

    @BeforeEach
    void setUp() {
        validator = new com.reserv.dataloader.validation.TransactionValidator(transactionService);
        processor = new TransactionsItemProcessor(validator, validationLogRepository, memcachedRepository, transactionsRepository);

        JobExecution jobExecution = new JobExecution(101L);
        StepExecution stepExecution = new StepExecution("transactionImportStep", jobExecution);
        processor.beforeStep(stepExecution);
    }

    @Test
    void testValidTransactionPassesSuccessfully() throws Exception {
        Transactions item = new Transactions();
        item.setName("Valid_Txn_Name");
        item.setIsGL(1);
        item.setIsReplayable(0);
        item.setExclusive(1);

        Transactions result = processor.process(item);

        assertNotNull(result);
        assertEquals("Valid_Txn_Name", result.getName());
        assertEquals(1, result.getIsGL());
        assertEquals(0, result.getIsReplayable());
        assertEquals(1, result.getExclusive());

        assertEquals(1, result.getExclusive());
    }

    @Test
    void testMissingNameReturnsNull() throws Exception {
        Transactions item = new Transactions();
        item.setName(""); // Empty
        item.setIsGL(1);
        item.setIsReplayable(1);

        Transactions result = processor.process(item);
        assertNull(result);
    }

    @Test
    void testDoubleSpacesInNameReturnsNull() throws Exception {
        Transactions item = new Transactions();
        item.setName("Invalid  Name"); // Double space
        item.setIsGL(1);
        item.setIsReplayable(1);

        Transactions result = processor.process(item);
        assertNull(result);
    }

    @Test
    void testSingleInternalSpaceInNameIsAccepted() throws Exception {
        Transactions item = new Transactions();
        item.setName("Loan Payment"); // Single internal space is allowed
        item.setIsGL(1);
        item.setIsReplayable(1);

        Transactions result = processor.process(item);
        assertNotNull(result);
        assertEquals("Loan Payment", result.getName());
    }

    @Test
    void testLeadingTrailingSpaceReturnsNull() throws Exception {
        Transactions item = new Transactions();
        item.setName(" Loan Payment");
        item.setIsGL(1);
        item.setIsReplayable(1);

        Transactions result = processor.process(item);
        assertNull(result);
    }

    @Test
    void testSpecialCharactersInNameReturnsNull() throws Exception {
        Transactions item = new Transactions();
        item.setName("Name@123"); 
        item.setIsGL(1);
        item.setIsReplayable(1);

        Transactions result = processor.process(item);
        assertNull(result);
    }

    @Test
    void testDuplicateNameInSameFileReturnsNull() throws Exception {
        Transactions item1 = new Transactions();
        item1.setName("DuplicateName");
        item1.setIsGL(1);
        item1.setIsReplayable(1);

        Transactions item2 = new Transactions();
        item2.setName("DuplicateName"); // Same name
        item2.setIsGL(1);
        item2.setIsReplayable(1);

        Transactions result1 = processor.process(item1);
        assertNotNull(result1);

        Transactions result2 = processor.process(item2);
        assertNull(result2, "Should return null to filter out file-level duplicates silently");
    }

    @Test
    void testDuplicateAlreadyInDbIsCaughtOnReUpload() throws Exception {
        // Simulate the exact bug scenario: same file uploaded a second time, so the
        // transaction name is already persisted in the DB from a prior job run.
        Transactions existingRecord = new Transactions();
        existingRecord.setName("DB_TXN_NAME");

        when(transactionsRepository.findAll()).thenReturn(Collections.singletonList(existingRecord));

        org.springframework.batch.core.JobParameters jobParams = new JobParametersBuilder()
                .addString("tenantId", "tenant123")
                .toJobParameters();
        JobExecution jobExecution = new JobExecution(103L, jobParams);
        StepExecution stepExecution = new StepExecution("transactionImportStep", jobExecution);
        processor.beforeStep(stepExecution);

        Transactions item = new Transactions();
        item.setName("DB_TXN_NAME"); // Matches preloaded DB record
        item.setIsGL(1);
        item.setIsReplayable(1);

        Transactions result = processor.process(item);

        assertNull(result, "Should filter out item whose name already exists in the database");
        verify(validationLogRepository, times(1)).saveAll(anyList());
    }

    @Test
    void testInvalidJournalReturnsNull() throws Exception {
        Transactions item = new Transactions();
        item.setName("ValidName");
        item.setIsGL(-1); // Invalid sentinel
        item.setIsReplayable(1);

        Transactions result = processor.process(item);
        assertNull(result);
    }

    @Test
    void testEmptyJournalDefaultsToTrueAndLogsWarning() throws Exception {
        Transactions item = new Transactions();
        item.setName("ValidName");
        item.setIsGL(-2); // Empty sentinel
        item.setIsReplayable(1);

        Transactions result = processor.process(item);

        assertNotNull(result);
        assertEquals(1, result.getIsGL()); // Defaulted to 1
        // Note: Warnings are no longer saved to DB directly in the processor. They are simply logged.
    }

    @Test
    void testBothFalseLogsWarning() throws Exception {
        Transactions item = new Transactions();
        item.setName("ValidName");
        item.setIsGL(0);
        item.setIsReplayable(0);

        Transactions result = processor.process(item);

        assertNotNull(result);
        // Note: Warnings are no longer saved to DB directly in the processor. They are simply logged.
    }
}
