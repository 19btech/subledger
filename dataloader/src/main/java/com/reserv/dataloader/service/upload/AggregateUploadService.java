package com.reserv.dataloader.service.upload;

import  com.fyntrac.common.enums.FileUploadActivityType;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
public class AggregateUploadService extends UploadService {
    @Autowired
    private Job aggregationUploadJob;

    @Autowired
    protected JobLauncher jobLauncher;

    public void uploadData(String filePath) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException, ExecutionException, InterruptedException {
        super.uploadData(jobLauncher, aggregationUploadJob, filePath, FileUploadActivityType.AGGREGATION);
    }
}