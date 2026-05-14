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
import org.springframework.batch.core.StepExecution;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class TransactionsItemProcessorTest {

    @Mock
    private com.fyntrac.common.service.TransactionService transactionService;

    private com.reserv.dataloader.validation.TransactionValidator validator;
    private TransactionsItemProcessor processor;

    @BeforeEach
    void setUp() {
        validator = new com.reserv.dataloader.validation.TransactionValidator(transactionService);
        processor = new TransactionsItemProcessor(validator);
        
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
    void testMissingNameReturnsNullAndLogsError() throws Exception {
        Transactions item = new Transactions();
        item.setName(""); // Empty
        item.setIsGL(1);
        item.setIsReplayable(1);

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> processor.process(item));

        assertEquals(1, ex.getValidationErrors().size());
        ItemValidationException.ValidationError log = ex.getValidationErrors().get(0);
        assertEquals("ERR_REQ_01", log.getErrorCode());
        assertEquals("ERROR", log.getSeverity());
    }

    @Test
    void testSpacesInNameReturnsNullAndLogsError() throws Exception {
        Transactions item = new Transactions();
        item.setName("Invalid Name"); 
        item.setIsGL(1);
        item.setIsReplayable(1);

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> processor.process(item));

        assertEquals(1, ex.getValidationErrors().size());
        ItemValidationException.ValidationError log = ex.getValidationErrors().get(0);
        assertEquals("ERR_SPC_01", log.getErrorCode());
        assertEquals("ERROR", log.getSeverity());
    }

    @Test
    void testSpecialCharactersInNameReturnsNullAndLogsError() throws Exception {
        Transactions item = new Transactions();
        item.setName("Name@123"); 
        item.setIsGL(1);
        item.setIsReplayable(1);

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> processor.process(item));

        assertEquals(1, ex.getValidationErrors().size());
        ItemValidationException.ValidationError log = ex.getValidationErrors().get(0);
        assertEquals("ERR_FMT_01", log.getErrorCode());
        assertEquals("ERROR", log.getSeverity());
    }

    @Test
    void testDuplicateNameInSameFileReturnsNullAndLogsError() throws Exception {
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

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> processor.process(item2));

        assertEquals(1, ex.getValidationErrors().size());
        ItemValidationException.ValidationError log = ex.getValidationErrors().get(0);
        assertEquals("ERR_DUP_01", log.getErrorCode());
        assertEquals("ERROR", log.getSeverity());
    }

    @Test
    void testInvalidJournalLogsErrorAndReturnsNull() throws Exception {
        Transactions item = new Transactions();
        item.setName("ValidName");
        item.setIsGL(-1); // Invalid sentinel
        item.setIsReplayable(1);

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> processor.process(item));

        assertEquals(1, ex.getValidationErrors().size());
        ItemValidationException.ValidationError log = ex.getValidationErrors().get(0);
        assertEquals("ERR_BOOL_01", log.getErrorCode());
        assertEquals("ERROR", log.getSeverity());
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
