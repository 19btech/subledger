package com.reserv.dataloader.service.upload;

import com.reserv.dataloader.datasource.accounting.rule.FileUploadActivityType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TransactionsUploadService  extends UploadService {
    @Autowired
    private Job transactionsUploadJob;
    @Autowired
    protected JobLauncher jobLauncher;


    public void uploadData(String filePath) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        super.uploadData(jobLauncher, transactionsUploadJob, filePath, FileUploadActivityType.TRANSACTION);
    }



}
