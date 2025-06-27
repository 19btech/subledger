package com.reserv.dataloader.batch.listener;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.enums.ActivityType;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.TransactionActivityService;
import com.reserv.dataloader.pulsar.producer.GeneralLedgerMessageProducer;
import com.reserv.dataloader.service.AggregationExecutionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.NoSuchElementException;

@Component
@Slf4j
public class ActivityReversalJobCompletionListener implements JobExecutionListener {

    private final AggregationExecutionService aggregationExecutionService;;

    private final ExecutionStateService executionStateService;

    private final JobLauncher jobLauncher;
    private final MemcachedRepository memcachedRepository;
    private final TransactionActivityService transactionActivityService;
    private final GeneralLedgerMessageProducer generalLedgerMessageProducer;


    @Autowired
    public ActivityReversalJobCompletionListener(AggregationExecutionService aggregationExecutionService
    , ExecutionStateService executionStateService
    , JobLauncher jobLauncher
    , MemcachedRepository memcachedRepository
    , TransactionActivityService transactionActivityService
    , GeneralLedgerMessageProducer generalLedgerMessageProducer) {
        this.aggregationExecutionService=aggregationExecutionService;
        this.executionStateService = executionStateService;
        this.jobLauncher = jobLauncher;
        this.memcachedRepository = memcachedRepository;
        this.transactionActivityService = transactionActivityService;
        this.generalLedgerMessageProducer = generalLedgerMessageProducer;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus().isUnsuccessful()) {
            return;
        }
        String lineSeparator = System.lineSeparator();
        String tenantId = jobExecution.getJobParameters().getString("tenantId");
        Long key = jobExecution.getJobParameters().getLong("run.id");
        Long jobId = jobExecution.getJobParameters().getLong("jobId");
        ExecutionState executionState = null;
        try {

            executionState = updateExecutionState(tenantId);

            assert executionState != null;

            Integer executionDate = executionState.getExecutionDate();
            Records.ExecuteAggregationMessageRecord aggregationMessageRecord = RecordFactory.createExecutionAggregationRecord(tenantId, jobId, (long) executionDate);
            aggregationExecutionService.execute(aggregationMessageRecord, executionState);
        } catch (Exception e) {
            this.memcachedRepository.flush(5);
            throw new RuntimeException(e);
        } finally {
            Records.GeneralLedgerMessageRecord glRec = RecordFactory.createGeneralLedgerMessageRecord(tenantId, jobId);
            generalLedgerMessageProducer.bookTempGL(glRec);
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("execution-date", (long) executionState.getExecutionDate())
                    .addString("activity-type", ActivityType.TRANSACTION_ACTIVITY.toString())
                    .toJobParameters();
        }
    }

    private ExecutionState updateExecutionState(String tenantId) {
        // Attempt to update the last accounting period of transaction activity upload
        try {
            // Retrieve the execution state directly (assuming it returns null if not found)
            ExecutionState executionState = this.executionStateService.getExecutionState();

            // Check if the execution state is not null
            if (executionState != null) {
                // Retrieve the latest activity posting date for the given tenant
                int latestPostingDate = this.transactionActivityService.getLatestActivityPostingDate(tenantId);

                // Update the activity posting date in the execution state
                if (latestPostingDate > executionState.getExecutionDate()) {
                    executionState.setLastExecutionDate(executionState.getExecutionDate());
                    executionState.setExecutionDate(latestPostingDate);
                    // Persist the updated execution state
                    return this.executionStateService.update(executionState);
                }
            } else {
                // Retrieve the latest activity posting date for the given tenant
                int latestPostingDate = this.transactionActivityService.getLatestActivityPostingDate(tenantId);

                // Create a new execution state since none was found
                ExecutionState executionStateNew = ExecutionState.builder()
                        .executionDate(latestPostingDate)
                        .lastExecutionDate(0)
                        .build();

                // Persist the new execution state
                log.warn("No execution state found for updating the last transaction activity posting date.");
                return this.executionStateService.update(executionStateNew);
            }
            return executionState;
        } catch (NoSuchElementException e) {
            log.error("Execution state not found when trying to update the last transaction activity upload reporting period.", e);
        } catch (Exception e) {
            log.error("Failed to update settings with the last transaction activity upload reporting period for tenantId: {}", tenantId, e);
        }

        return null;
    }
}


