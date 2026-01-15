package com.reserv.dataloader.batch.listener;

import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.enums.FileUploadActivityType;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.*;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.reserv.dataloader.entity.ActivityLog;
import com.reserv.dataloader.pulsar.producer.GeneralLedgerMessageProducer;
import com.reserv.dataloader.service.CacheService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class TransactionActivityJobCompletionListener implements JobExecutionListener {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    MemcachedRepository memcachedRepository;

    @Autowired
    GeneralLedgerMessageProducer generalLedgerMessageProducer;

    @Autowired
    AccountingPeriodService accountingPeriodService;

    @Autowired
    AggregationService aggregationService;

    @Autowired
    CacheService cacheService;

    @Autowired
    TransactionService transactionService;

    @Autowired
    Job reversalJob;

    @Autowired
    Job instrumentReplayStateJob;

    private final ExecutionStateService executionStateService;
    private final TransactionActivityService transactionActivityService;
    private final DataService<ActivityLog> dataService;
    @Autowired
    public TransactionActivityJobCompletionListener(ExecutionStateService executionStateService,
                                                    TransactionActivityService transactionActivityService,
                                                    DataService<ActivityLog> dataService) {
        this.executionStateService = executionStateService;
        this.transactionActivityService = transactionActivityService;
        this.dataService = dataService;
    }
    @Override
    public void beforeJob(JobExecution jobExecution) {
        // No-op
        this.memcachedRepository.flush(0);

        try {
            this.accountingPeriodService.loadIntoCache();
            this.aggregationService.loadIntoCache();
            this.cacheService.loadIntoCache();
            this.transactionService.loadIntoCache();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Optional: log or handle interrupt as needed
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        StringBuilder jobExecutionMessage = new StringBuilder(0);
        String lineSeparator = System.lineSeparator();
        String tenantId = jobExecution.getJobParameters().getString("tenantId");
        Long key = jobExecution.getJobParameters().getLong("run.id");
        Long jobId = jobExecution.getJobParameters().getLong("jobId");
        Long uploadId = jobExecution.getJobParameters().getLong("uploadId");
        String tableName = "TransactionActivity";
        FileUploadActivityType activityType = FileUploadActivityType.TRANSACTION_ACTIVITY;

        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            jobExecutionMessage.append("Job Status : ")
                    .append(jobExecution.getStatus())
                    .append(lineSeparator).append("Job Name : ")
                    .append(jobExecution.getJobInstance().getJobName())
                    .append(lineSeparator).append("Job Start Time : ")
                    .append(jobExecution.getStartTime())
                    .append(lineSeparator).append("Job End Time : ")
                    .append(jobExecution.getEndTime())
                    .append(lineSeparator)
                    .append("Upload File Name : ").
                    append(jobExecution.getJobParameters().getString("fileName"))
                    .append(lineSeparator)
                    .append("Step execution details : ")
                    .append(jobExecution.getStepExecutions().toString());

                try {
                    log.info(jobExecutionMessage.toString());

                    assert tenantId != null;
                    assert jobId != null;
                    JobParameters jobParameters = new JobParametersBuilder()
                            .addString("tenantId", tenantId)
                            .addLong("jobId", jobId)
                            .addLong("key", key)
                            .toJobParameters();
                    ExecutionState executionState =  updateExecutionState(tenantId);

                     jobLauncher.run(instrumentReplayStateJob, jobParameters);
                    jobLauncher.run(reversalJob, jobParameters);

                    assert executionState != null;
                    this.logActivity(uploadId, executionState.getExecutionDate(), tableName,jobExecution,
                            activityType);
                } catch (Exception e) {
                    this.memcachedRepository.flush(5);
                    throw new RuntimeException(e);
                }


        }else if(jobExecution.getStatus() == BatchStatus.FAILED) {
            jobExecutionMessage.append("Job Status : ")
                    .append(jobExecution.getStatus())
                    .append(lineSeparator).append("Job Name : ")
                    .append(jobExecution.getJobInstance().getJobName())
                    .append(lineSeparator).append("Job Start Time : ")
                    .append(jobExecution.getStartTime())
                    .append(lineSeparator).append("Job End Time : ")
                    .append(jobExecution.getEndTime())
                    .append(lineSeparator)
                    .append("Upload File Name : ").
                    append(jobExecution.getJobParameters().getString("fileName"))
                    .append(lineSeparator)
                    .append("Step execution details : ")
                    .append(jobExecution.getStepExecutions().toString());

            log.error(jobExecutionMessage.toString());
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
                int latestPostingDate = this.transactionActivityService.getMaxPostingDate();

                // Update the activity posting date in the execution state
                if (latestPostingDate > executionState.getExecutionDate()) {
                    executionState.setLastExecutionDate(executionState.getExecutionDate());
                    executionState.setExecutionDate(latestPostingDate);
                    // Persist the updated execution state
                    return this.executionStateService.update(executionState);
                }
            } else {
                // Retrieve the latest activity posting date for the given tenant
                int latestPostingDate = this.transactionActivityService.getMaxPostingDate();

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

    private void logActivity(long uploadId, Integer postingDate, String tableName, JobExecution execution,
                             FileUploadActivityType activityType) {

        // Loop through steps to get detailed stats
        List<ActivityLog> logs = new ArrayList<>(0);
        ExecutionState executionState = this.executionStateService.getExecutionState();
        for (StepExecution stepExecution : execution.getStepExecutions()) {

            ActivityLog activityLog = ActivityLog.builder()
                    .activityType(activityType)
                    .jobName(execution.getJobInstance().getJobName())
                    .tableName(tableName)
                    // 2. Use Step times for precision (or Job times if tracking whole job)
                    .startingTime(stepExecution.getStartTime())
                    .endingTime(stepExecution.getEndTime()) // Note: might be null if called in 'beforeStep'

                    // 3. robust parameter retrieval
                    .uploadId(uploadId)
                    .jobId(execution.getJobParameters().getLong("run.id"))
                    .uploadFilePath(execution.getJobParameters().getString("filePath"))

                    // 4. Improved Status Logic (Checks Step status specifically)
                    .activityStatus(!stepExecution.getFailureExceptions().isEmpty() ? "FAILED" : stepExecution.getExitStatus().getExitCode())

                    // 5. CRITICAL: Don't save the 'stepExecution' object. Save the stats instead.
                    .recordsRead(stepExecution.getReadCount())
                    .recordsWritten(stepExecution.getWriteCount())
                    .recordsSkipped(stepExecution.getSkipCount())
                    .postingDate(executionState.getExecutionDate())
                    // 6. Capture Error Message if exists
                    .errorMessage(stepExecution.getFailureExceptions().isEmpty() ? null : stepExecution.getFailureExceptions().get(0).getMessage())

                    .build();

            logs.add(activityLog);
        }


        dataService.saveAll(logs, ActivityLog.class);

    }

}
