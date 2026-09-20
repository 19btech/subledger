package com.reserv.dataloader.service;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.entity.Errors;
import com.fyntrac.common.enums.ErrorCategory;
import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.enums.ErrorType;
import com.fyntrac.common.service.ErrorService;
import com.fyntrac.common.service.TransactionActivityService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

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

    @Autowired
    private ErrorService errorService;

    public void execute(Records.ExecuteAggregationMessageRecord msg, ExecutionState executionState)
            throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException,
            JobParametersInvalidException, JobRestartException {
        JobParameters jobParameters = executeInstrumentScoped(msg, executionState);
        executeMetricLevel(jobParameters, msg);
    }

    /**
     * Runs the attribute-level then instrument-level LTD jobs for one batch. Both write rows keyed
     * by instrument (plus attribute/metric), and an instrument is dispatched in exactly one batch
     * per run (see ExcelModelService.generateEventAndDispatch), so batches processing concurrently
     * never read-modify-write the same row here — callers may run this for several batches at
     * once. The metric-level job is deliberately NOT part of this: see {@link #executeMetricLevel}.
     *
     * @return the JobParameters to pass on to {@link #executeMetricLevel} for the same batch
     */
    public JobParameters executeInstrumentScoped(Records.ExecuteAggregationMessageRecord msg, ExecutionState executionState)
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

        JobExecution attributeJobExecution = jobLauncher.run(attributeLevelLtdJob, jobParameters);
        waitForJobCompletion(attributeJobExecution);
        if (attributeJobExecution.getStatus() != BatchStatus.COMPLETED) {
            throw new RuntimeException("Attribute aggregation job failed. "
                    + recordJobFailure("attributeLevelLtdJob", attributeJobExecution, msg));
        }
        log.info("Attributes aggregation job execution has been successfully completed");

        JobExecution instrumentJobExecution = jobLauncher.run(instrumentLevelLtdJob, jobParameters);
        waitForJobCompletion(instrumentJobExecution);
        if (instrumentJobExecution.getStatus() != BatchStatus.COMPLETED) {
            throw new RuntimeException("Instrument aggregation job failed. "
                    + recordJobFailure("instrumentLevelLtdJob", instrumentJobExecution, msg));
        }
        log.info("Instruments aggregation job execution has been successfully completed");
        return jobParameters;
    }

    /**
     * Runs the metric-level LTD job for one batch. MetricLevelLtdFlatteningWriter aggregates ONE
     * row per (metricName, postingDate) across ALL instruments, and the balance fields are
     * read-modify-written (they're stored as strings, so no atomic $inc is possible) — two batches
     * running this concurrently would silently drop one batch's contribution. Callers must
     * serialize this across batches of the same run.
     */
    public void executeMetricLevel(JobParameters jobParameters, Records.ExecuteAggregationMessageRecord msg)
            throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException,
            JobParametersInvalidException, JobRestartException {
        JobExecution metricJobExecution = jobLauncher.run(metricLevelLtdJob, jobParameters);
        waitForJobCompletion(metricJobExecution);
        if (metricJobExecution.getStatus() != BatchStatus.COMPLETED) {
            throw new RuntimeException("Metric aggregation job failed. "
                    + recordJobFailure("metricLevelLtdJob", metricJobExecution, msg));
        }
        log.info("Metrics aggregation job execution has been successfully completed");
    }

    /**
     * Pulls the actual failure detail off a non-COMPLETED JobExecution (Spring Batch swallows this
     * once the caller only checks getStatus()), logs it, and persists it to the Errors collection
     * so it survives past this JVM's log retention. Returns a short summary to fold into the
     * RuntimeException thrown to the caller.
     */
    private String recordJobFailure(String jobName, JobExecution jobExecution, Records.ExecuteAggregationMessageRecord msg) {
        List<Throwable> failureExceptions = jobExecution.getAllFailureExceptions();
        String detail = failureExceptions.isEmpty()
                ? "no exception captured; exitStatus=" + jobExecution.getExitStatus()
                : StringUtil.getStackTrace(failureExceptions.get(0));

        log.error("{} execution failed for tenant {} jobId {} with status: {}, exitStatus: {}, failureExceptions: {}",
                jobName, msg.tenantId(), msg.jobId(), jobExecution.getStatus(), jobExecution.getExitStatus(), failureExceptions);

        try {
            Errors error = Errors.builder()
                    .jobId(String.valueOf(msg.jobId()))
                    .postingDate(DateUtil.convertToDateFromYYYYMMDD(msg.aggregationDate().intValue()))
                    .executionDate(DateUtil.convertToDateFromYYYYMMDD(msg.aggregationDate().intValue()))
                    .code(ErrorCode.Aggregation_Execution_Error)
                    .errorCategory(ErrorCategory.PROCESSING)
                    .errorType(ErrorType.ERROR)
                    .isWarning(Boolean.FALSE)
                    .message(jobName + " failed for tenant " + msg.tenantId()
                            + " with status " + jobExecution.getStatus()
                            + " (exitStatus: " + jobExecution.getExitStatus().getExitCode() + ")")
                    .stacktrace(detail)
                    .build();
            this.errorService.save(error);
        } catch (Exception e) {
            log.error("Failed to persist {} failure to Errors collection for tenant {}: {}", jobName, msg.tenantId(), e.getMessage(), e);
        }

        return "status=" + jobExecution.getStatus() + ", cause=" + (failureExceptions.isEmpty() ? "unknown" : failureExceptions.get(0));
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
