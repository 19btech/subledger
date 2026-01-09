package com.reserv.dataloader.service.upload;

import com.fyntrac.common.enums.FileUploadActivityType;
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

@Service
@Slf4j
public class InstrumentBalanceUploadService extends UploadService {

    @Autowired
    private Job instrumentBalanceUploadJob;

    @Autowired
    protected JobLauncher jobLauncher;

    public void uploadData(long uploadId,String filePath) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException
    {
        LocalDateTime startingTime = DateUtil.getDateTime();
        long runid = System.currentTimeMillis();
        StringBuilder columnNames = new StringBuilder();
        //List<String> columnNames = new ArrayList<>(0);

        columnNames.append("ACTIVITYUPLOADID:NUMBER");
        columnNames.append(",METRICNAME:STRING");
        columnNames.append(",METRIC:STRING");
        columnNames.append(",INSTRUMENTID:STRING");
        columnNames.append(",POSTINGDATE:NUMBER");
        columnNames.append(",ACTIVITYAMOUNT:NUMBER");
        columnNames.append(",BEGINNINGBALANCE:NUMBER");
        columnNames.append(",ENDINGBALANCE:NUMBER");

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("filePath", filePath)
                .addString("columnName", columnNames.toString())
                .addLong("run.id", runid)
                .toJobParameters();
        super.uploadData(uploadId,jobLauncher
                ,instrumentBalanceUploadJob
                ,jobParameters
                ,runid
                ,startingTime
                ,filePath
                , FileUploadActivityType.INSTRUMENT_BALANCE
        );
    }
}

