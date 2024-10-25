package com.reserv.dataloader.service.upload;

import  com.fyntrac.common.enums.FileUploadActivityType;
import com.fyntrac.common.entity.Attributes;
import com.reserv.dataloader.service.AttributeService;
import com.fyntrac.common.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Collection;

@Service
@Slf4j
public class ChartOfAccountUploadService extends UploadService {
    @Autowired
    private Job chartOfAccountUploadJob;

    @Autowired
    protected JobLauncher jobLauncher;
    @Autowired
    AttributeService attributeService;

    public void uploadData(String filePath) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        LocalDateTime startingTime = DateUtil.getDateTime();
        long runid = System.currentTimeMillis();
        StringBuilder columnNames = new StringBuilder();
        //List<String> columnNames = new ArrayList<>(0);

        columnNames.append("ACTIVITYUPLOADID:NUMBER");
        columnNames.append(",ACCOUNTNUMBER:STRING");
        columnNames.append(",ACCOUNTNAME:STRING");
        columnNames.append(",ACCOUNTSUBTYPE:STRING");

        Collection<Attributes> attributes = attributeService.getReclassableAttributes();
        for(Attributes attribute : attributes) {
            columnNames.append(",");
            columnNames.append(attribute.getAttributeName()).append(":").append(attribute.getDataType());
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("filePath", filePath)
                .addString("columnName", columnNames.toString())
                .addLong("run.id", runid)
                .toJobParameters();
        super.uploadData(jobLauncher
                ,chartOfAccountUploadJob
                ,jobParameters
                ,runid
                ,startingTime
                ,filePath
                ,FileUploadActivityType.CHART_OF_ACCOUNT
        );
    }
}
