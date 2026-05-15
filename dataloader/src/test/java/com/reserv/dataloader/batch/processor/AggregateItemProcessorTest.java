package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.Aggregation;
import com.fyntrac.common.entity.Transactions;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.repository.TransactionsRepo;
import com.reserv.dataloader.validation.AggregationValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AggregateItemProcessorTest {

    @Mock
    private com.fyntrac.common.service.TransactionService transactionService;

    @Mock
    private com.fyntrac.common.service.aggregation.AggregationService aggregationService;

    @Mock
    private com.fyntrac.common.repository.RefDataValidationLogRepository validationLogRepository;

    private AggregationValidator validator;
    private AggregateItemProcessor processor;

    @BeforeEach
    void setUp() {
        validator = new AggregationValidator(transactionService, aggregationService);
        processor = new AggregateItemProcessor(validator, transactionService, aggregationService, validationLogRepository);

        // Mock database preloading (Transactions)
        Transactions tx1 = Transactions.builder().name("PAYMENT").build();
        Transactions tx2 = Transactions.builder().name("REVERSAL").build();
        when(transactionService.getAll()).thenReturn(Arrays.asList(tx1, tx2));

        // Mock database preloading (Existing Metrics)
        Aggregation agg1 = Aggregation.builder().metricName("PRE_EXISTING_METRIC").transactionName("PAYMENT").build();
        when(aggregationService.fetchAll()).thenReturn(Arrays.asList(agg1));

        // Initialize chunk listener metadata and trigger preloading caching routine
        org.springframework.batch.core.JobParameters jobParams = new org.springframework.batch.core.JobParametersBuilder()
                .addString("tenantId", "TNT_UNIT_TEST")
                .toJobParameters();
        JobExecution jobExecution = new JobExecution(1001L, jobParams);
        StepExecution stepExecution = new StepExecution("aggregationImportStep", jobExecution);
        processor.beforeStep(stepExecution);
    }

    @Test
    void testValidAggregationPasses() throws Exception {
        Aggregation item = new Aggregation();
        item.setTransactionName("PAYMENT");
        item.setMetricName("TOTAL_AMOUNT");

        Aggregation result = processor.process(item);

        assertNotNull(result);
        assertEquals("PAYMENT", result.getTransactionName());
        assertEquals("TOTAL_AMOUNT", result.getMetricName());
    }

    @Test
    void testTransactionNameNullReturnsNull() throws Exception {
        Aggregation item = new Aggregation();
        item.setTransactionName(null);
        item.setMetricName("TOTAL_AMOUNT");

        Aggregation result = processor.process(item);
        assertNull(result);
    }

    @Test
    void testTransactionNameNotFoundInDbReturnsNull() throws Exception {
        Aggregation item = new Aggregation();
        item.setTransactionName("FEE_CHARGE"); // Does not exist in preloaded set
        item.setMetricName("TOTAL_AMOUNT");

        Aggregation result = processor.process(item);
        assertNull(result);
    }

    @Test
    void testTransactionNameWithLeadingTrailingSpacesReturnsNull() throws Exception {
        Aggregation item = new Aggregation();
        item.setTransactionName(" PAYMENT "); // Has space padding
        item.setMetricName("TOTAL_AMOUNT");

        Aggregation result = processor.process(item);
        assertNull(result);
    }

    @Test
    void testMetricNameNullReturnsNull() throws Exception {
        Aggregation item = new Aggregation();
        item.setTransactionName("PAYMENT");
        item.setMetricName(null);

        Aggregation result = processor.process(item);
        assertNull(result);
    }

    @Test
    void testMetricNameWithSpacesReturnsNull() throws Exception {
        Aggregation item = new Aggregation();
        item.setTransactionName("PAYMENT");
        item.setMetricName("TOTAL AMOUNT"); // Contains Spaces

        Aggregation result = processor.process(item);
        assertNull(result);
    }

    @Test
    void testMetricNameWithSpecialCharactersReturnsNull() throws Exception {
        Aggregation item = new Aggregation();
        item.setTransactionName("PAYMENT");
        item.setMetricName("AMOUNT@USD"); // Special Character

        Aggregation result = processor.process(item);
        assertNull(result);
    }

    @Test
    void testMetricNameAlreadyExistsInDbReturnsNull() throws Exception {
        Aggregation item = new Aggregation();
        item.setTransactionName("PAYMENT");
        item.setMetricName("PRE_EXISTING_METRIC"); // Exists in DB Mock

        Aggregation result = processor.process(item);
        assertNull(result);
    }

    @Test
    void testMetricNameDuplicateInFileReturnsNull() throws Exception {
        // First record passes
        Aggregation item1 = new Aggregation();
        item1.setTransactionName("PAYMENT");
        item1.setMetricName("NEW_UNIQUE_METRIC");
        Aggregation result1 = processor.process(item1);
        assertNotNull(result1);

        // Identical metric with a DIFFERENT transaction name also passes now (composite key logic)
        Aggregation item2 = new Aggregation();
        item2.setTransactionName("REVERSAL");
        item2.setMetricName("NEW_UNIQUE_METRIC");
        Aggregation result2 = processor.process(item2);
        assertNotNull(result2);

        // Identical metric with the SAME transaction name in another record must return null (ignored/skipped)
        Aggregation item3 = new Aggregation();
        item3.setTransactionName("PAYMENT");
        item3.setMetricName("NEW_UNIQUE_METRIC");

        Aggregation result3 = processor.process(item3);
        assertNull(result3, "Should return null to filter out file-level duplicates silently");
    }
}
