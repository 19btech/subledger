package com.reserv.dataloader.service;

import com.reserv.dataloader.datasource.accounting.rule.FileUploadActivityType;
import com.reserv.dataloader.entity.ActivityLog;
import com.reserv.dataloader.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;

@Slf4j
public abstract class UploadService {

    @Autowired
    private DataService dataService;

    public abstract void uploadData(String filePath) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException;
    public void uploadData(JobLauncher jobLauncher, Job job, String filePath, FileUploadActivityType activityType) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException{
        LocalDateTime startingTime = DateUtil.getDateTime();
        long runid = System.currentTimeMillis();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("filePath", filePath)
                    .addLong("run.id", runid)
                    .toJobParameters();
        this.uploadData(jobLauncher
                ,job
                ,jobParameters
                ,runid
                ,startingTime
                ,filePath
                ,activityType
                );
    }

    public void uploadData(JobLauncher jobLauncher
            , Job job
            , JobParameters jobParameters
            , long runid
            , LocalDateTime startingTime
            , String filePath
            , FileUploadActivityType activityType) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException{
        try {
            JobExecution execution = jobLauncher.run(job, jobParameters);
            this.logActivity(execution, activityType);
            log.info("Exit Status : " + execution.getStatus());
        }catch (Exception e ){
            log.error(e.getMessage());
            log.error(e.getCause().getMessage());
            logActivityException(e, startingTime,runid, filePath, activityType);
        }
    }

    private void logActivity(JobExecution execution, FileUploadActivityType activityType) {
        ActivityLog activityLog = ActivityLog.builder().
                activityType(activityType)
                .jobName(execution.getJobInstance().getJobName())
                .startingTime(execution.getStartTime())
                .endingTime(execution.getEndTime())
                .jobId(execution.getJobId())
                .uploadFilePath(execution.getJobParameters().getString("filePath"))
                .activityStatus(execution.getExitStatus().getExitCode())
                .build();
        dataService.save(activityLog);

    }

    private void logActivityException(Exception exp, LocalDateTime startingtime,long runId, String filePath, FileUploadActivityType activityType) {
        LocalDateTime endingTime = DateUtil.getDateTime();
        ActivityLog activityLog = ActivityLog.builder().
                activityType(activityType)
                .jobName(activityType.toString())
                .startingTime(startingtime)
                .endingTime(endingTime)
                .jobId(runId)
                .uploadFilePath(filePath)
                .activityStatus("FAILED")
                .build();
        dataService.save(activityLog);

    }
}
