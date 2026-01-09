package com.reserv.dataloader.service.upload;

import  com.fyntrac.common.enums.FileUploadActivityType;
import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.service.AttributeService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.service.AccountingPeriodDataUploadService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class InstrumentAttributeUploadService extends UploadService {
    @Autowired
    private Job instrumentAttributeUploadJob;

    @Autowired
    protected JobLauncher jobLauncher;

    @Autowired
    AttributeService attributeService;

    private final AccountingPeriodDataUploadService accountingPeriodService;

    @Autowired
    InstrumentAttributeUploadService(AccountingPeriodDataUploadService accountingPeriodService) {
        this.accountingPeriodService = accountingPeriodService;
    }

    public void uploadData(long uploadId,String filePath) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException, ExecutionException, InterruptedException {
        LocalDateTime startingTime = DateUtil.getDateTime();
        long runid = System.currentTimeMillis();
        StringBuilder columnNames = new StringBuilder();

        columnNames.append("ACTIVITYUPLOADID:NUMBER");
        columnNames.append(",POSTINGDATE:DATE");
        columnNames.append(",EFFECTIVEDATE:DATE");
        columnNames.append(",ATTRIBUTEID:STRING");
        columnNames.append(",INSTRUMENTID:STRING");

        Collection<Attributes> attributes = attributeService.getAllAttributes();
        for(Attributes attribute : attributes) {
            columnNames.append(",");
            columnNames.append(attribute.getAttributeName()).append(":").append(attribute.getDataType());
        }
        accountingPeriodService.loadIntoCache();
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("filePath", filePath)
                .addString("columnName", columnNames.toString())
                .addLong("run.id", runid)
                .addString("tenantId", this.dataService.getTenantId())
                .toJobParameters();
        super.uploadData(uploadId,jobLauncher
                ,instrumentAttributeUploadJob
                ,jobParameters
                ,runid
                ,startingTime
                ,filePath
                ,FileUploadActivityType.INSTRUMENT_ATTRIBUTE
        );
    }
}
