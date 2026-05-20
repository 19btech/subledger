package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.ActivityDataValidationLog;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.entity.factory.InstrumentAttributeFactory;
import com.fyntrac.common.repository.ActivityDataValidationLogRepository;
import com.fyntrac.common.repository.AttributesRepository;
import com.reserv.dataloader.service.ActivityValidationLogService;
import com.reserv.dataloader.validation.InstrumentAttributeValidator;
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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InstrumentAttributeItemProcessorTest {

    @Mock private AttributesRepository attributesRepository;
    @Mock private ActivityDataValidationLogRepository activityLogRepository;
    @Mock private InstrumentAttributeFactory instrumentAttributeFactory;

    private ActivityValidationLogService validationLogService;
    private InstrumentAttributeItemProcessor processor;

    @BeforeEach
    void setUp() {
        InstrumentAttributeValidator validator =
                new InstrumentAttributeValidator(attributesRepository);
        validationLogService = new ActivityValidationLogService(activityLogRepository);
        processor = new InstrumentAttributeItemProcessor(
                validator, validationLogService, attributesRepository);

        // Inject mocked factory via reflection (@Autowired field)
        try {
            java.lang.reflect.Field f = InstrumentAttributeItemProcessor.class
                    .getDeclaredField("instrumentAttributeFactory");
            f.setAccessible(true);
            f.set(processor, instrumentAttributeFactory);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // attributesRepository.findAll() used in @BeforeStep preload — return empty list
        when(attributesRepository.findAll()).thenReturn(List.of());

        JobParameters params = new JobParametersBuilder()
                .addString("tenantId", "test_tenant")
                .toJobParameters();
        JobExecution jobExecution = new JobExecution(202L, params);
        StepExecution stepExecution =
                new StepExecution("instrumentAttributeImportStep", jobExecution);
        processor.beforeStep(stepExecution);
    }

    // -----------------------------------------------------------------------
    // Happy path
    // -----------------------------------------------------------------------

    @Test
    void testValidRowLoadsSuccessfully() throws Exception {
        Map<String, Object> item = new HashMap<>();
        item.put("POSTINGDATE",   "05/19/2026");
        item.put("EFFECTIVEDATE", "05/18/2026");
        item.put("INSTRUMENTID",  "INST-001");
        item.put("ATTRIBUTEID",   "ATTR-001");
        item.put("USERID",        "user_abc");

        InstrumentAttribute mockEntity = new InstrumentAttribute();
        mockEntity.setInstrumentId("INST-001");
        mockEntity.setAttributeId("ATTR-001");

        when(instrumentAttributeFactory.create(
                eq("test_tenant"), eq("INST-001"), eq("ATTR-001"),
                any(Date.class), eq(0), eq(20260519), any(), any()))
                .thenReturn(mockEntity);

        InstrumentAttribute result = processor.process(item);

        assertNotNull(result);
        assertEquals("INST-001", result.getInstrumentId());
        verify(activityLogRepository, never()).saveAll(any());
    }

    // -----------------------------------------------------------------------
    // ID normalisation (.0 decimal suffix from Excel)
    // -----------------------------------------------------------------------

    @Test
    void testDecimalSuffixIdsAreNormalised() throws Exception {
        Map<String, Object> item = new HashMap<>();
        item.put("POSTINGDATE",   "05/19/2026");
        item.put("EFFECTIVEDATE", "05/18/2026");
        item.put("INSTRUMENTID",  "101.0");   // normalised → "101"
        item.put("ATTRIBUTEID",   "1.0");     // NOT normalised — preserved as "1.0"

        InstrumentAttribute mockEntity = new InstrumentAttribute();
        mockEntity.setInstrumentId("101");
        mockEntity.setAttributeId("1.0");

        when(instrumentAttributeFactory.create(
                eq("test_tenant"), eq("101"), eq("1.0"),
                any(Date.class), eq(0), eq(20260519), any(), any()))
                .thenReturn(mockEntity);

        InstrumentAttribute result = processor.process(item);

        assertNotNull(result);
        assertEquals("101",  result.getInstrumentId());
        assertEquals("1.0",  result.getAttributeId());
    }

    // -----------------------------------------------------------------------
    // Mandatory date fields
    // -----------------------------------------------------------------------

    @Test
    void testMissingPostingDateLogsError() throws Exception {
        Map<String, Object> item = new HashMap<>();
        // POSTINGDATE absent
        item.put("EFFECTIVEDATE", "05/18/2026");
        item.put("INSTRUMENTID",  "INST-002");

        InstrumentAttribute result = processor.process(item);
        assertNull(result);

        ArgumentCaptor<List<ActivityDataValidationLog>> captor = ArgumentCaptor.forClass(List.class);
        verify(activityLogRepository, times(1)).saveAll(captor.capture());

        List<ActivityDataValidationLog> logs = captor.getValue();
        assertEquals(1, logs.size());
        assertEquals("POSTINGDATE", logs.get(0).getFieldName());
        assertEquals("ERR_REQ_02",  logs.get(0).getErrorCode());
    }

    @Test
    void testMissingEffectiveDateLogsError() throws Exception {
        Map<String, Object> item = new HashMap<>();
        item.put("POSTINGDATE",  "05/19/2026");
        // EFFECTIVEDATE absent
        item.put("INSTRUMENTID", "INST-002");

        InstrumentAttribute result = processor.process(item);
        assertNull(result);

        ArgumentCaptor<List<ActivityDataValidationLog>> captor = ArgumentCaptor.forClass(List.class);
        verify(activityLogRepository, times(1)).saveAll(captor.capture());

        List<ActivityDataValidationLog> logs = captor.getValue();
        assertEquals(1, logs.size());
        assertEquals("EFFECTIVEDATE", logs.get(0).getFieldName());
        assertEquals("ERR_REQ_02",    logs.get(0).getErrorCode());
    }

    // -----------------------------------------------------------------------
    // Date format errors
    // -----------------------------------------------------------------------

    @Test
    void testInvalidDateFormatLogsError() throws Exception {
        Map<String, Object> item = new HashMap<>();
        item.put("POSTINGDATE",   "not-a-date");
        item.put("EFFECTIVEDATE", "20260518");   // YYYYMMDD — rejected
        item.put("INSTRUMENTID",  "INST-003");

        InstrumentAttribute result = processor.process(item);
        assertNull(result);

        ArgumentCaptor<List<ActivityDataValidationLog>> captor = ArgumentCaptor.forClass(List.class);
        verify(activityLogRepository, times(1)).saveAll(captor.capture());

        List<ActivityDataValidationLog> logs = captor.getValue();
        assertEquals(2, logs.size());
        assertTrue(logs.stream().anyMatch(l ->
                "POSTINGDATE".equals(l.getFieldName()) && "ERR_FMT_DT".equals(l.getErrorCode())));
        assertTrue(logs.stream().anyMatch(l ->
                "EFFECTIVEDATE".equals(l.getFieldName()) && "ERR_FMT_DT".equals(l.getErrorCode())));
    }

    // -----------------------------------------------------------------------
    // Mandatory INSTRUMENTID
    // -----------------------------------------------------------------------

    @Test
    void testMissingInstrumentIdLogsError() throws Exception {
        Map<String, Object> item = new HashMap<>();
        item.put("POSTINGDATE",   "05/19/2026");
        item.put("EFFECTIVEDATE", "05/18/2026");
        // INSTRUMENTID absent

        InstrumentAttribute result = processor.process(item);
        assertNull(result);

        ArgumentCaptor<List<ActivityDataValidationLog>> captor = ArgumentCaptor.forClass(List.class);
        verify(activityLogRepository, times(1)).saveAll(captor.capture());

        List<ActivityDataValidationLog> logs = captor.getValue();
        assertEquals(1, logs.size());
        assertEquals("INSTRUMENTID", logs.get(0).getFieldName());
        assertEquals("ERR_REQ_03",   logs.get(0).getErrorCode());
    }

    // -----------------------------------------------------------------------
    // Leading/trailing spaces on INSTRUMENTID
    // -----------------------------------------------------------------------

    @Test
    void testSpacesInInstrumentIdLogsError() throws Exception {
        Map<String, Object> item = new HashMap<>();
        item.put("POSTINGDATE",   "05/19/2026");
        item.put("EFFECTIVEDATE", "05/18/2026");
        item.put("INSTRUMENTID",  " INST-003 ");   // spaces

        InstrumentAttribute result = processor.process(item);
        assertNull(result);

        ArgumentCaptor<List<ActivityDataValidationLog>> captor = ArgumentCaptor.forClass(List.class);
        verify(activityLogRepository, times(1)).saveAll(captor.capture());

        List<ActivityDataValidationLog> logs = captor.getValue();
        assertEquals(1, logs.size());
        assertEquals("INSTRUMENTID", logs.get(0).getFieldName());
        assertEquals("ERR_SPC_01",   logs.get(0).getErrorCode());
    }
}
