package com.reserv.dataloader.batch.listener;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.service.*;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.Key;
import com.reserv.dataloader.pulsar.producer.GeneralLedgerMessageProducer;
import com.fyntrac.common.repository.MemcachedRepository;
import com.reserv.dataloader.service.AggregationExecutionService;
import com.reserv.dataloader.service.CacheService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.fyntrac.common.dto.record.RecordFactory;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class TransactionActivityJobCompletionListener implements JobExecutionListener {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private Job instrumentAggregationJob;

    @Autowired
    private Job attributeAggregationJob;

    @Autowired
    private Job metricAggregationJob;

    @Autowired
    private Job updateInstrumentActivitySateJob;

    @Autowired
    MemcachedRepository memcachedRepository;

    @Autowired
    TransactionActivityService transactionActivityService;

    @Autowired
    ExecutionStateService executionStateService;

    @Autowired
    DataService<com.fyntrac.common.entity.Settings> dataService;

    @Autowired
    GeneralLedgerMessageProducer generalLedgerMessageProducer;

    @Autowired
    AggregationExecutionService aggregationExecutionService;
    @Autowired
    AccountingPeriodService accountingPeriodService;

    @Autowired
    AggregationService aggregationService;

    @Autowired
    CacheService cacheService;

    @Autowired
    TransactionService transactionService;

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
        String replayInstrumentDataKey = Key.replayMessageList(tenantId, key);
        CacheList<Records.TransactionActivityReplayRecord> replayList = this.memcachedRepository.getFromCache(replayInstrumentDataKey, CacheList.class);
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

            if (jobExecution.getStatus() == BatchStatus.COMPLETED) {


                try {
                    log.info(jobExecutionMessage.toString());

                    ExecutionState executionState =  updateExecutionState(tenantId);

                    assert executionState != null;

                    String aggregationKey = com.fyntrac.common.utils.Key.aggregationKey(tenantId, key);
                    Integer executionDate = executionState.getActivityPostingDate();
                    Records.ExecuteAggregationMessageRecord aggregationMessageRecord = RecordFactory.createExecutionAggregationRecord(tenantId, aggregationKey, (long) executionDate);
                    aggregationExecutionService.execute(aggregationMessageRecord, executionState);

                    Long runId = System.currentTimeMillis();
                    JobParameters jobParameters = new JobParametersBuilder()
                            .addLong("run.id", runId)
                            .addString("aggregation-key", com.fyntrac.common.utils.Key.aggregationKey(tenantId, key))
                            .addLong("execution-date", aggregationMessageRecord.aggregationDate())
                            .toJobParameters();

//                    jobLauncher.run(attributeAggregationJob, jobParameters);
//                    jobLauncher.run(instrumentAggregationJob, jobParameters);
//                    jobLauncher.run(metricAggregationJob, jobParameters);
                    jobLauncher.run(updateInstrumentActivitySateJob, jobParameters);
                } catch (Exception e) {
                    this.memcachedRepository.flush(5);
                    throw new RuntimeException(e);
                }finally {
                    Records.GeneralLedgerMessageRecord glRec = RecordFactory.createGeneralLedgerMessageRecord(tenantId, com.fyntrac.common.utils.Key.aggregationKey(tenantId, key));
                    generalLedgerMessageProducer.bookTempGL(glRec);

                   // memcachedRepository.delete(aggregationRequest.getKey());
                }
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
                int latestPostingDate = this.transactionActivityService.getLatestActivityPostingDate(tenantId);

                // Update the activity posting date in the execution state
                if (latestPostingDate > executionState.getActivityPostingDate()) {
                    executionState.setLastActivityPostingDate(executionState.getActivityPostingDate());
                    executionState.setActivityPostingDate(latestPostingDate);
                    // Persist the updated execution state
                    return this.executionStateService.update(executionState);
                }
            } else {
                // Retrieve the latest activity posting date for the given tenant
                int latestPostingDate = this.transactionActivityService.getLatestActivityPostingDate(tenantId);

                // Create a new execution state since none was found
                ExecutionState executionStateNew = ExecutionState.builder()
                        .activityPostingDate(latestPostingDate)
                        .lastActivityPostingDate(0)
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
