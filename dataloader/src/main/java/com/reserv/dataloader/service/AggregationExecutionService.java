package com.reserv.dataloader.service;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.service.TransactionActivityService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AggregationExecutionService {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job instrumentLevelLtdJob;

    @Autowired
    private Job attributeLevelLtdJob;

    @Autowired
    private Job metricLevelLtdJob;

    @Autowired
    private Job attributeLevelPostAggregationJob;

    @Autowired
    private Job instrumentLevelPostAggregationJob;

    @Autowired
    private Job metricLevelPostAggregationJob;

    @Autowired
    TransactionActivityService transactionActivityService;

    public void execute(Records.ExecuteAggregationMessageRecord msg, ExecutionState executionState)
            throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException,
            JobParametersInvalidException, JobRestartException {

        Long runId = System.currentTimeMillis();

        Integer previousMaxPostingDate = this.transactionActivityService.getPreviousMaxPostingDate(msg.aggregationDate());
        previousMaxPostingDate = previousMaxPostingDate == null ? 0 : previousMaxPostingDate;
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("run.id", runId)
                .addLong("execution-date", msg.aggregationDate())
                .addLong("previousMaxPostingDate", (long) previousMaxPostingDate)
                .addLong("jobId", msg.jobId())
                .addString("tenantId", msg.tenantId())
                .toJobParameters();

        // Run the first 3 jobs sequentially
        JobExecution attributeJobExecution = jobLauncher.run(attributeLevelLtdJob, jobParameters);
        waitForJobCompletion(attributeJobExecution);
        if (attributeJobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("Attributes aggregation job execution has been successfully completed");
            JobExecution instrumentJobExecution = jobLauncher.run(instrumentLevelLtdJob, jobParameters);
            waitForJobCompletion(instrumentJobExecution);
            if (instrumentJobExecution.getStatus() == BatchStatus.COMPLETED) {
                log.info("Instruments aggregation job execution has been successfully completed");
                JobExecution metricJobExecution = jobLauncher.run(metricLevelLtdJob, jobParameters);
                waitForJobCompletion(metricJobExecution);
                if (metricJobExecution.getStatus() == BatchStatus.COMPLETED) {
                    log.info("Metrics aggregation job execution has been successfully completed");
                } else {
                    log.error("Metrics aggregation job execution failed with status: {}", metricJobExecution.getStatus());
                    throw new RuntimeException("Metric aggregation job failed.");
                }
            } else {
                log.error("Instruments aggregation job execution failed with status: {}", instrumentJobExecution.getStatus());
                throw new RuntimeException("Instrument aggregation job failed.");
            }
        } else {
            log.error("Attributes aggregation job execution failed with status: {}", attributeJobExecution.getStatus());
            throw new RuntimeException("Attribute aggregation job failed.");
        }
    }

    private void waitForJobCompletion(JobExecution jobExecution) {
        try {
            while (jobExecution.isRunning()) {
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for job to complete", e);
        }
    }

    public void executePostAggregation(Records.ExecuteAggregationMessageRecord msg, ExecutionState executionState)
            throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException,
            JobParametersInvalidException, JobRestartException {

        Long runId = System.currentTimeMillis();

        Integer previousMaxPostingDate = this.transactionActivityService.getPreviousMaxPostingDate(msg.aggregationDate());
        previousMaxPostingDate = previousMaxPostingDate == null ? 0 : previousMaxPostingDate;
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("run.id", runId)
                .addLong("execution-date", msg.aggregationDate())
                .addLong("previousMaxPostingDate", (long) previousMaxPostingDate)
                .addLong("jobId", msg.jobId())
                .addString("tenantId", msg.tenantId())
                .toJobParameters();

        // Run the first 3 jobs sequentially

                    if (previousMaxPostingDate != null && previousMaxPostingDate > 0) {


                        long postingDate = msg.aggregationDate();
                        JobParameters postAggregationJobParameters = new JobParametersBuilder()
                                .addLong("run.id", System.currentTimeMillis())
                                .addLong("executionDate", postingDate)
                                .addLong("fromDate", (long) previousMaxPostingDate)
                                .addLong("jobId", msg.jobId())
                                .addString("tenantId", msg.tenantId())
                                .toJobParameters();

                        // ✅ All 3 jobs succeeded — now run the final job
                        JobExecution attributeLevelPostAggregationJobExecution = jobLauncher.run(attributeLevelPostAggregationJob, postAggregationJobParameters);
                        waitForJobCompletion(attributeLevelPostAggregationJobExecution);
                        if (attributeLevelPostAggregationJobExecution.getStatus() == BatchStatus.COMPLETED) {
                            log.info("Attributes post aggregation job execution has been successfully completed");
                            JobExecution instrumentLevelPostAggregationJobExecution = jobLauncher.run(instrumentLevelPostAggregationJob, postAggregationJobParameters);
                            waitForJobCompletion(instrumentLevelPostAggregationJobExecution);
                            if (instrumentLevelPostAggregationJobExecution.getStatus() == BatchStatus.COMPLETED) {
                                log.info("Instruments post aggregation job execution has been successfully completed");
                                JobExecution metricLevelPostAggregationJobExecution = jobLauncher.run(metricLevelPostAggregationJob, postAggregationJobParameters);
                                waitForJobCompletion(metricLevelPostAggregationJobExecution);
                                if (metricLevelPostAggregationJobExecution.getStatus() != BatchStatus.COMPLETED) {
                                    throw new RuntimeException("Metric Level post aggregation job failed with status: " + metricLevelPostAggregationJobExecution.getStatus());
                                }else {
                                    log.info("Metrics post aggregation job execution has been successfully completed");
                                }
                            } else {
                                throw new RuntimeException("Instrument Level post aggregation job failed with status: " + instrumentLevelPostAggregationJobExecution.getStatus());
                            }
                        } else {
                            throw new RuntimeException("Attribute Level post aggregation job failed with status: " + attributeLevelPostAggregationJobExecution.getStatus());
                        }

                    }



    }
}
