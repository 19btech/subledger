package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.entity.RefDataValidationLog;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.repository.InstrumentAttributeRepository;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import com.fyntrac.common.service.TransactionService;
import com.reserv.dataloader.validation.TransactionActivityValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TransactionActivityItemProcessorTest {

    @Mock private TransactionService transactionService;
    @Mock private InstrumentAttributeRepository instrumentAttributeRepository;
    @Mock private RefDataValidationLogRepository validationLogRepository;

    private TransactionActivityItemProcessor processor;

    private static final String VALID_INSTRUMENT_ID  = "INST-001";
    private static final String VALID_ATTRIBUTE_ID   = "1.0";
    private static final String VALID_TX_NAME        = "TX-ALPHA";

    @BeforeEach
    void setUp() {
        TransactionActivityValidator validator = new TransactionActivityValidator();
        processor = new TransactionActivityItemProcessor(
                validator, transactionService, instrumentAttributeRepository, validationLogRepository);

        // Stub InstrumentAttribute preload
        InstrumentAttribute ia = new InstrumentAttribute();
        ia.setInstrumentId(VALID_INSTRUMENT_ID);
        ia.setAttributeId(VALID_ATTRIBUTE_ID);
        when(instrumentAttributeRepository.findAll()).thenReturn(List.of(ia));

        // Stub Transactions preload — getAll() returns Collection
        Transactions tx = new Transactions();
        tx.setName(VALID_TX_NAME);
        when(transactionService.getAll()).thenReturn((Collection) List.of(tx));

        JobParameters params = new JobParametersBuilder()
                .addString("tenantId", "test_tenant")
                .toJobParameters();
        JobExecution jobExecution = new JobExecution(301L, params);
        StepExecution stepExecution =
                new StepExecution("transactionActivityImportStep", jobExecution);
        processor.beforeStep(stepExecution);
    }

    // -----------------------------------------------------------------------
    // Happy path
    // -----------------------------------------------------------------------

    @Test
    void testValidRowLoadsSuccessfully() throws Exception {
        Map<String, Object> item = buildValidItem();

        TransactionActivity result = processor.process(item);

        assertNotNull(result);
        assertEquals(VALID_INSTRUMENT_ID, result.getInstrumentId());
        assertEquals(VALID_ATTRIBUTE_ID,  result.getAttributeId());
        assertEquals(VALID_TX_NAME,       result.getTransactionName());
        assertTrue(result.getValidationErrors().isEmpty());
        verify(validationLogRepository, never()).saveAll(any());
    }

    // -----------------------------------------------------------------------
    // Missing mandatory dates
    // -----------------------------------------------------------------------

    @Test
    void testMissingPostingDateLogsError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.remove("POSTINGDATE");

        assertNullWithSingleError(item, "POSTINGDATE", "ERR_REQ_02");
    }

    @Test
    void testMissingTransactionDateLogsError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.remove("TRANSACTIONDATE");

        assertNullWithSingleError(item, "TRANSACTIONDATE", "ERR_REQ_02");
    }

    // -----------------------------------------------------------------------
    // Invalid date formats
    // -----------------------------------------------------------------------

    @Test
    void testInvalidPostingDateFormatLogsError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.put("POSTINGDATE", "not-a-date");

        assertNullWithSingleError(item, "POSTINGDATE", "ERR_FMT_DT");
    }

    @Test
    void testInvalidTransactionDateFormatLogsError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.put("TRANSACTIONDATE", "20260519");   // YYYYMMDD — rejected

        assertNullWithSingleError(item, "TRANSACTIONDATE", "ERR_FMT_DT");
    }

    // -----------------------------------------------------------------------
    // INSTRUMENTID
    // -----------------------------------------------------------------------

    @Test
    void testMissingInstrumentIdLogsError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.remove("INSTRUMENTID");

        assertNullWithSingleError(item, "INSTRUMENTID", "ERR_REQ_03");
    }

    @Test
    void testInstrumentIdSpacesLogsError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.put("INSTRUMENTID", " INST-001 ");

        assertNullWithSingleError(item, "INSTRUMENTID", "ERR_SPC_01");
    }

    @Test
    void testUnknownInstrumentIdLogsRefError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.put("INSTRUMENTID", "UNKNOWN");

        assertNullWithSingleError(item, "INSTRUMENTID", "ERR_REF_08");
    }

    // -----------------------------------------------------------------------
    // ATTRIBUTEID
    // -----------------------------------------------------------------------

    @Test
    void testMissingAttributeIdLogsError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.remove("ATTRIBUTEID");

        assertNullWithSingleError(item, "ATTRIBUTEID", "ERR_REQ_03");
    }

    @Test
    void testAttributeIdSpacesLogsError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.put("ATTRIBUTEID", " 1.0 ");

        assertNullWithSingleError(item, "ATTRIBUTEID", "ERR_SPC_01");
    }

    @Test
    void testUnknownAttributeIdLogsRefError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.put("ATTRIBUTEID", "UNKNOWN-ATTR");

        assertNullWithSingleError(item, "ATTRIBUTEID", "ERR_REF_09");
    }

    // -----------------------------------------------------------------------
    // TRANSACTIONNAME
    // -----------------------------------------------------------------------

    @Test
    void testMissingTransactionNameLogsError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.remove("TRANSACTIONNAME");

        assertNullWithSingleError(item, "TRANSACTIONNAME", "ERR_REQ_01");
    }

    @Test
    void testUnknownTransactionNameLogsRefError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.put("TRANSACTIONNAME", "NO-SUCH-TX");

        assertNullWithSingleError(item, "TRANSACTIONNAME", "ERR_REF_01");
    }

    // -----------------------------------------------------------------------
    // AMOUNT
    // -----------------------------------------------------------------------

    @Test
    void testMissingAmountLogsError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.remove("AMOUNT");

        assertNullWithSingleError(item, "AMOUNT", "ERR_REQ_01");
    }

    @Test
    void testInvalidAmountLogsError() throws Exception {
        Map<String, Object> item = buildValidItem();
        item.put("AMOUNT", "not-a-number");

        assertNullWithSingleError(item, "AMOUNT", "ERR_FMT_DEC");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Build a fully valid CSV row map. */
    private Map<String, Object> buildValidItem() {
        Map<String, Object> item = new HashMap<>();
        item.put("POSTINGDATE",    "05/19/2026");
        item.put("TRANSACTIONDATE", "05/18/2026");
        item.put("INSTRUMENTID",   VALID_INSTRUMENT_ID);
        item.put("ATTRIBUTEID",    VALID_ATTRIBUTE_ID);
        item.put("TRANSACTIONNAME", VALID_TX_NAME);
        item.put("AMOUNT",         "100.50");
        return item;
    }

    /**
     * Assert that the processor returns null and logs exactly one error
     * matching the expected column and error code.
     */
    private void assertNullWithSingleError(Map<String, Object> item,
                                            String expectedColumn,
                                            String expectedErrorCode) throws Exception {
        TransactionActivity result = processor.process(item);
        assertNull(result, "Expected processor to return null for invalid row");

        ArgumentCaptor<List<RefDataValidationLog>> captor = ArgumentCaptor.forClass(List.class);
        verify(validationLogRepository, atLeastOnce()).saveAll(captor.capture());

        List<RefDataValidationLog> logs = captor.getValue();
        assertTrue(
                logs.stream().anyMatch(l ->
                        expectedColumn.equalsIgnoreCase(l.getSourceColumn())
                        && expectedErrorCode.equals(l.getErrorCode())),
                "Expected error " + expectedErrorCode + " on column " + expectedColumn
                        + " but got: " + logs);
    }
}
