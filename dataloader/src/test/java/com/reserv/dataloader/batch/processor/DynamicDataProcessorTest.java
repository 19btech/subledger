package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.ActivityDataValidationLog;
import com.fyntrac.common.entity.CustomTableColumn;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.repository.ActivityDataValidationLogRepository;
import com.fyntrac.common.repository.InstrumentAttributeRepository;
import com.reserv.dataloader.service.ActivityValidationLogService;
import com.reserv.dataloader.validation.DynamicTableValidator;
import org.bson.Document;
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
import org.springframework.batch.item.file.transform.DefaultFieldSet;
import org.springframework.batch.item.file.transform.FieldSet;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DynamicDataProcessorTest {

    @Mock private InstrumentAttributeRepository instrumentAttributeRepository;
    @Mock private ActivityDataValidationLogRepository activityLogRepository;

    private ActivityValidationLogService validationLogService;
    private DynamicDataProcessor processor;
    private CustomTableDefinition tableDef;

    private static final String VALID_INSTRUMENT_ID = "INST-001";
    private static final String VALID_ATTRIBUTE_ID  = "1.0";

    @BeforeEach
    void setUp() {
        // Build a minimal CustomTableDefinition with system columns + custom columns
        tableDef = new CustomTableDefinition("CUSTOMER_DATA", "Test table", null);
        // System columns (always present)
        tableDef.addColumn(new CustomTableColumn("InstrumentId", "INSTRUMENTID", CustomTableColumn.DataType.STRING, false, 1));
        tableDef.addColumn(new CustomTableColumn("AttributeId",  "ATTRIBUTEID",  CustomTableColumn.DataType.STRING, false, 2));
        tableDef.addColumn(new CustomTableColumn("PostingDate",  "POSTINGDATE",  CustomTableColumn.DataType.DATE,   false, 3));
        tableDef.addColumn(new CustomTableColumn("EffectiveDate","EFFECTIVEDATE",CustomTableColumn.DataType.DATE,   false, 4));
        // Custom business columns
        tableDef.addColumn(new CustomTableColumn("Name",   "name",   CustomTableColumn.DataType.STRING, true, 5));
        tableDef.addColumn(new CustomTableColumn("Amount", "amount", CustomTableColumn.DataType.NUMBER, true, 6));

        DynamicTableValidator validator = new DynamicTableValidator();
        validationLogService = new ActivityValidationLogService(activityLogRepository);
        processor = new DynamicDataProcessor(tableDef, validator,
                instrumentAttributeRepository, validationLogService);

        // Stub preload
        InstrumentAttribute ia = new InstrumentAttribute();
        ia.setInstrumentId(VALID_INSTRUMENT_ID);
        ia.setAttributeId(VALID_ATTRIBUTE_ID);
        when(instrumentAttributeRepository.findAll()).thenReturn(List.of(ia));

        JobParameters params = new JobParametersBuilder()
                .addString("tenantId", "test_tenant")
                .toJobParameters();
        JobExecution jobExecution = new JobExecution(401L, params);
        StepExecution stepExecution = new StepExecution("dynamicLoadStep", jobExecution);
        processor.beforeStep(stepExecution);
    }

    // -----------------------------------------------------------------------
    // Happy path
    // -----------------------------------------------------------------------

    @Test
    void testValidRowProducesDocument() throws Exception {
        FieldSet fieldSet = buildFieldSet(
                VALID_INSTRUMENT_ID, VALID_ATTRIBUTE_ID,
                "05/19/2026", "05/18/2026",
                "TestName", "123.45");

        Document result = processor.process(fieldSet);

        assertNotNull(result);
        verify(activityLogRepository, never()).saveAll(any());
    }

    // -----------------------------------------------------------------------
    // Mandatory date fields
    // -----------------------------------------------------------------------

    @Test
    void testMissingPostingDateLogsError() throws Exception {
        FieldSet fieldSet = buildFieldSet(VALID_INSTRUMENT_ID, VALID_ATTRIBUTE_ID,
                "", "05/18/2026", "TestName", "10.00");

        Document result = processor.process(fieldSet);
        assertNull(result);
        assertErrorCode("POSTINGDATE", "ERR_REQ_02");
    }

    @Test
    void testInvalidPostingDateFormatLogsError() throws Exception {
        FieldSet fieldSet = buildFieldSet(VALID_INSTRUMENT_ID, VALID_ATTRIBUTE_ID,
                "20260519", "05/18/2026", "TestName", "10.00");

        Document result = processor.process(fieldSet);
        assertNull(result);
        assertErrorCode("POSTINGDATE", "ERR_FMT_DT");
    }

    @Test
    void testMissingEffectiveDateLogsError() throws Exception {
        FieldSet fieldSet = buildFieldSet(VALID_INSTRUMENT_ID, VALID_ATTRIBUTE_ID,
                "05/19/2026", "", "TestName", "10.00");

        Document result = processor.process(fieldSet);
        assertNull(result);
        assertErrorCode("EFFECTIVEDATE", "ERR_REQ_02");
    }

    // -----------------------------------------------------------------------
    // INSTRUMENTID
    // -----------------------------------------------------------------------

    @Test
    void testMissingInstrumentIdLogsError() throws Exception {
        FieldSet fieldSet = buildFieldSet("", VALID_ATTRIBUTE_ID,
                "05/19/2026", "05/18/2026", "TestName", "10.00");

        Document result = processor.process(fieldSet);
        assertNull(result);
        assertErrorCode("INSTRUMENTID", "ERR_REQ_03");
    }

    // Note: DefaultFieldSet.readString() auto-trims values, so leading/trailing space
    // detection on FieldSet-based processors is handled at the CSV reader level, not here.

    @Test
    void testUnknownInstrumentIdLogsRefError() throws Exception {
        FieldSet fieldSet = buildFieldSet("UNKNOWN", VALID_ATTRIBUTE_ID,
                "05/19/2026", "05/18/2026", "TestName", "10.00");

        Document result = processor.process(fieldSet);
        assertNull(result);
        assertErrorCode("INSTRUMENTID", "ERR_REF_03");
    }

    // -----------------------------------------------------------------------
    // ATTRIBUTEID
    // -----------------------------------------------------------------------

    @Test
    void testMissingAttributeIdLogsError() throws Exception {
        FieldSet fieldSet = buildFieldSet(VALID_INSTRUMENT_ID, "",
                "05/19/2026", "05/18/2026", "TestName", "10.00");

        Document result = processor.process(fieldSet);
        assertNull(result);
        assertErrorCode("ATTRIBUTEID", "ERR_REQ_03");
    }

    @Test
    void testUnknownAttributeIdLogsRefError() throws Exception {
        FieldSet fieldSet = buildFieldSet(VALID_INSTRUMENT_ID, "UNKNOWN-ATTR",
                "05/19/2026", "05/18/2026", "TestName", "10.00");

        Document result = processor.process(fieldSet);
        assertNull(result);
        assertErrorCode("ATTRIBUTEID", "ERR_REF_03");
    }

    // -----------------------------------------------------------------------
    // Datatype validation
    // -----------------------------------------------------------------------

    @Test
    void testInvalidNumberColumnLogsTypeError() throws Exception {
        FieldSet fieldSet = buildFieldSet(VALID_INSTRUMENT_ID, VALID_ATTRIBUTE_ID,
                "05/19/2026", "05/18/2026", "TestName", "not-a-number");

        Document result = processor.process(fieldSet);
        assertNull(result);
        assertErrorCode("amount", "ERR_TYPE_01");
    }

    // -----------------------------------------------------------------------
    // Duplicate row detection
    // -----------------------------------------------------------------------

    @Test
    void testFullyIdenticalRowIsFlaggerAsDuplicate() throws Exception {
        FieldSet row = buildFieldSet(VALID_INSTRUMENT_ID, VALID_ATTRIBUTE_ID,
                "05/19/2026", "05/18/2026", "TestName", "10.00");

        // First row — accepted
        Document first = processor.process(row);
        assertNotNull(first);

        // Second row with every field identical → duplicate
        FieldSet dup = buildFieldSet(VALID_INSTRUMENT_ID, VALID_ATTRIBUTE_ID,
                "05/19/2026", "05/18/2026", "TestName", "10.00");
        Document second = processor.process(dup);
        assertNull(second);
        assertErrorCode("INSTRUMENTID", "ERR_DUP_03");
    }

    @Test
    void testRowsWithDifferentColumnValueAreNotDuplicates() throws Exception {
        // Same instrumentId/attributeId/postingDate but 'name' differs — must NOT be a duplicate
        FieldSet row1 = buildFieldSet(VALID_INSTRUMENT_ID, VALID_ATTRIBUTE_ID,
                "05/19/2026", "05/18/2026", "Name-A", "10.00");
        FieldSet row2 = buildFieldSet(VALID_INSTRUMENT_ID, VALID_ATTRIBUTE_ID,
                "05/19/2026", "05/18/2026", "Name-B", "10.00");

        Document first  = processor.process(row1);
        Document second = processor.process(row2);

        assertNotNull(first,  "First row should be accepted");
        assertNotNull(second, "Second row differs in 'name' column and must NOT be flagged as duplicate");
        verify(activityLogRepository, never()).saveAll(any());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Builds a FieldSet with all 6 columns defined in the tableDef.
     * Column names must exactly match those in the CustomTableDefinition.
     */
    private FieldSet buildFieldSet(String instrumentId, String attributeId,
                                   String postingDate, String effectiveDate,
                                   String name, String amount) {
        String[] names  = {"INSTRUMENTID", "ATTRIBUTEID", "POSTINGDATE", "EFFECTIVEDATE", "name", "amount"};
        String[] values = { instrumentId,   attributeId,   postingDate,   effectiveDate,   name,   amount };
        return new DefaultFieldSet(values, names);
    }

    private void assertErrorCode(String expectedColumn, String expectedCode) {
        ArgumentCaptor<List<ActivityDataValidationLog>> captor = ArgumentCaptor.forClass(List.class);
        verify(activityLogRepository, atLeastOnce()).saveAll(captor.capture());

        List<ActivityDataValidationLog> logs = captor.getValue();
        assertTrue(
                logs.stream().anyMatch(l ->
                        expectedColumn.equalsIgnoreCase(l.getFieldName())
                        && expectedCode.equals(l.getErrorCode())),
                "Expected error " + expectedCode + " on column " + expectedColumn
                        + " but got: " + logs);
    }
}
