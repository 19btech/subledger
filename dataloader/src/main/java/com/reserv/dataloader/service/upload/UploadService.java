package com.reserv.dataloader.service.upload;

import  com.fyntrac.common.enums.FileUploadActivityType;
import com.reserv.dataloader.entity.ActivityLog;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
public abstract class UploadService {

    @Autowired
    protected DataService<ActivityLog> dataService;
    protected Long runId;

    public abstract void uploadData(long uploadId, String filePath) throws JobInstanceAlreadyCompleteException,
            JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException, ExecutionException, InterruptedException;

    public void uploadData(long uploadId,JobLauncher jobLauncher, Job job, String filePath, FileUploadActivityType activityType) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException, ExecutionException, InterruptedException {
        LocalDateTime startingTime = DateUtil.getDateTime();
        this.runId = System.currentTimeMillis();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("filePath", filePath)
                    .addString("tenantId", this.dataService.getTenantId())
                    .addLong("run.id", this.runId)
                    .toJobParameters();
        this.uploadData(uploadId, jobLauncher
                ,job
                ,jobParameters
                ,runId
                ,startingTime
                ,filePath
                ,activityType
                );
    }

    public void uploadData(long uploadId,JobLauncher jobLauncher
            , Job job
            , JobParameters jobParameters
            , long runid
            , LocalDateTime startingTime
            , String filePath
            , FileUploadActivityType activityType) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException{
        JobExecution execution = null;
        try {
            execution = jobLauncher.run(job, jobParameters);
            this.logActivity(uploadId, activityType.name(),execution, activityType);
            log.info("Exit Status : " + execution.getStatus());
        }catch (Exception e ){
            log.error(e.getMessage());
            log.error(e.getCause().getMessage());
            logActivity(uploadId, activityType.name(),execution, activityType);
        }
    }

    private void logActivity(long uploadId, String tableName, JobExecution execution,
                             FileUploadActivityType activityType) {

        // Loop through steps to get detailed stats
        List<ActivityLog> logs = new ArrayList<>(0);

        for (StepExecution stepExecution : execution.getStepExecutions()) {

            ActivityLog activityLog = ActivityLog.builder()
                    .activityType(activityType)
                    .jobName(execution.getJobInstance().getJobName())
                    .tableName(tableName)
                    // 2. Use Step times for precision (or Job times if tracking whole job)
                    .startingTime(stepExecution.getStartTime())
                    .endingTime(stepExecution.getEndTime()) // Note: might be null if called in 'beforeStep'

                    // 3. robust parameter retrieval
                    .jobId(execution.getJobParameters().getLong("run.id"))
                    .uploadId(uploadId)
                    .uploadFilePath(execution.getJobParameters().getString("filePath"))

                    // 4. Improved Status Logic (Checks Step status specifically)
                    .activityStatus(!stepExecution.getFailureExceptions().isEmpty() ? "FAILED" : stepExecution.getExitStatus().getExitCode())

                    // 5. CRITICAL: Don't save the 'stepExecution' object. Save the stats instead.
                    .recordsRead(stepExecution.getReadCount())
                    .recordsWritten(stepExecution.getWriteCount())
                    .recordsSkipped(stepExecution.getSkipCount())

                    // 6. Capture Error Message if exists
                    .errorMessage(stepExecution.getFailureExceptions().isEmpty() ? null : stepExecution.getFailureExceptions().get(0).getMessage())

                    .build();

            logs.add(activityLog);
        }


        dataService.saveAll(logs, ActivityLog.class);

    }

}
