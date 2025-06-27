package com.reserv.dataloader.batch.listener;

import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.TransactionService;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.reserv.dataloader.pulsar.producer.GeneralLedgerMessageProducer;
import com.reserv.dataloader.service.CacheService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

                     jobLauncher.run(reversalJob, jobParameters);
                     jobLauncher.run(instrumentReplayStateJob, jobParameters);
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


}
