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
        if (!attributeJobExecution.getStatus().isUnsuccessful()) {
            JobExecution instrumentJobExecution = jobLauncher.run(instrumentLevelLtdJob, jobParameters);
            if (!instrumentJobExecution.getStatus().isUnsuccessful()) {
                JobExecution metricJobExecution = jobLauncher.run(metricLevelLtdJob, jobParameters);
                if (!metricJobExecution.getStatus().isUnsuccessful()) {


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
                        if (!attributeLevelPostAggregationJobExecution.getStatus().isUnsuccessful()) {
                            JobExecution instrumentLevelPostAggregationJobExecution = jobLauncher.run(instrumentLevelPostAggregationJob, postAggregationJobParameters);
                            if (!instrumentLevelPostAggregationJobExecution.getStatus().isUnsuccessful()) {
                                JobExecution metricLevelPostAggregationJobExecution = jobLauncher.run(metricLevelPostAggregationJob, postAggregationJobParameters);
                                if (metricLevelPostAggregationJobExecution.getStatus().isUnsuccessful()) {
                                    throw new RuntimeException("Metric Level post aggregation job failed.");

                                }
                            } else {
                                throw new RuntimeException("Instrument Level post aggregation job failed.");
                            }
                        } else {
                            throw new RuntimeException("Attribute Level post aggregation job failed.");
                        }

                    }
                } else {
                    throw new RuntimeException("Metric aggregation job failed.");
                }

            } else {
                throw new RuntimeException("Instrument aggregation job failed.");
            }
        } else {
            throw new RuntimeException("Attribute aggregation job failed.");
        }
    }
}
