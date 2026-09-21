package com.reserv.dataloader.service;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.service.ErrorService;
import com.fyntrac.common.service.TransactionActivityService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AggregationExecutionServiceTest {

    @Mock private JobLauncher jobLauncher;
    @Mock private Job attributeLevelLtdJob;
    @Mock private Job instrumentLevelLtdJob;
    @Mock private Job metricLevelLtdJob;
    @Mock private TransactionActivityService transactionActivityService;
    @Mock private ErrorService errorService;

    private AggregationExecutionService service;
    private final Records.ExecuteAggregationMessageRecord msg =
            new Records.ExecuteAggregationMessageRecord("TNT", 42L, 20250630L);

    @BeforeEach
    void setUp() {
        service = new AggregationExecutionService();
        ReflectionTestUtils.setField(service, "jobLauncher", jobLauncher);
        ReflectionTestUtils.setField(service, "attributeLevelLtdJob", attributeLevelLtdJob);
        ReflectionTestUtils.setField(service, "instrumentLevelLtdJob", instrumentLevelLtdJob);
        ReflectionTestUtils.setField(service, "metricLevelLtdJob", metricLevelLtdJob);
        ReflectionTestUtils.setField(service, "transactionActivityService", transactionActivityService);
        ReflectionTestUtils.setField(service, "errorService", errorService);
        lenient().when(transactionActivityService.getPreviousMaxPostingDate(any())).thenReturn(20250531);
    }

    @Test
    void instrumentScopedPhaseRunsAttributeThenInstrumentAndNeverMetric() throws Exception {
        when(jobLauncher.run(any(), any())).thenReturn(completed());

        JobParameters params = service.executeInstrumentScoped(msg, new ExecutionState());

        InOrder inOrder = inOrder(jobLauncher);
        inOrder.verify(jobLauncher).run(eq(attributeLevelLtdJob), any());
        inOrder.verify(jobLauncher).run(eq(instrumentLevelLtdJob), any());
        verify(jobLauncher, never()).run(eq(metricLevelLtdJob), any());
        assertEquals(42L, params.getLong("jobId"));
        assertEquals("TNT", params.getString("tenantId"));
    }

    @Test
    void metricPhaseReusesTheBatchParameters() throws Exception {
        when(jobLauncher.run(any(), any())).thenReturn(completed());
        JobParameters params = service.executeInstrumentScoped(msg, new ExecutionState());

        service.executeMetricLevel(params, msg);

        verify(jobLauncher).run(metricLevelLtdJob, params);
    }

    @Test
    void executeRunsAllThreeInOrder() throws Exception {
        when(jobLauncher.run(any(), any())).thenReturn(completed());

        service.execute(msg, new ExecutionState());

        InOrder inOrder = inOrder(jobLauncher);
        inOrder.verify(jobLauncher).run(eq(attributeLevelLtdJob), any());
        inOrder.verify(jobLauncher).run(eq(instrumentLevelLtdJob), any());
        inOrder.verify(jobLauncher).run(eq(metricLevelLtdJob), any());
    }

    @Test
    void attributeFailureStopsBeforeInstrumentJob() throws Exception {
        when(jobLauncher.run(eq(attributeLevelLtdJob), any())).thenReturn(failed());

        assertThrows(RuntimeException.class, () -> service.executeInstrumentScoped(msg, new ExecutionState()));

        verify(jobLauncher, never()).run(eq(instrumentLevelLtdJob), any());
        verify(jobLauncher, never()).run(eq(metricLevelLtdJob), any());
    }

    private static JobExecution completed() {
        JobExecution execution = new JobExecution(1L);
        execution.setStatus(BatchStatus.COMPLETED);
        execution.setExitStatus(ExitStatus.COMPLETED);
        return execution;
    }

    private static JobExecution failed() {
        JobExecution execution = new JobExecution(2L);
        execution.setStatus(BatchStatus.FAILED);
        execution.setExitStatus(ExitStatus.FAILED);
        return execution;
    }
}
