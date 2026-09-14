package com.reserv.dataloader.service.upload;

import  com.fyntrac.common.enums.FileUploadActivityType;
import com.fyntrac.common.repository.AccountTypesRepository;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class AccountTypeUploadService extends UploadService {
    @Autowired
    private Job accountTypeUploadJob;

    @Autowired
    protected JobLauncher jobLauncher;

    @Autowired
    AccountTypesRepository accountTypesRepository;

    public void uploadData(boolean isOverwrite,long uploadId, String filePath) throws JobInstanceAlreadyCompleteException,
            JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException, ExecutionException, InterruptedException {
        if (isOverwrite) {
            log.info("Overwrite requested: purging existing AccountTypes before reload.");
            accountTypesRepository.deleteAll();
        }
        super.uploadData(uploadId,jobLauncher, accountTypeUploadJob, filePath, FileUploadActivityType.ACCOUNT_TYPE);
    }
}
