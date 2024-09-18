package com.reserv.dataloader.service.upload;

import com.reserv.dataloader.datasource.accounting.rule.AggregationRequestType;
import com.reserv.dataloader.datasource.accounting.rule.FileUploadActivityType;
import com.reserv.dataloader.entity.AggregationRequest;
import com.reserv.dataloader.service.AccountingPeriodService;
import com.reserv.dataloader.service.AggregationRequestService;
import com.reserv.dataloader.service.CacheService;
import com.reserv.dataloader.service.TransactionActivityService;
import com.reserv.dataloader.service.aggregation.AggregationService;
import com.reserv.dataloader.utils.DateUtil;
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
public class TransactionActivityUploadService extends UploadService {
    @Autowired
    private Job transactionActivityUploadJob;

    @Autowired
    AggregationRequestService metricAggregationRequestService;

    @Autowired
    protected JobLauncher jobLauncher;
    private final AccountingPeriodService accountingPeriodService;
    private final AggregationService aggregationService;
    private final TransactionActivityService transactionActivityService;
    private final CacheService cacheService;
    @Autowired
    public TransactionActivityUploadService(AccountingPeriodService accountingPeriodService
                                            ,TransactionActivityService transactionActivityService
    , AggregationService aggregationService
    , CacheService cacheService) {
        this.accountingPeriodService = accountingPeriodService;
        this.aggregationService = aggregationService;
        this.transactionActivityService = transactionActivityService;
        this.cacheService = cacheService;
    }
    public void uploadData(String filePath) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        this.uploadData(jobLauncher, transactionActivityUploadJob, filePath, FileUploadActivityType.TRANSACTION_ACTIVITY);
    }

    @Override
    public void uploadData(JobLauncher jobLauncher, Job job, String filePath, FileUploadActivityType activityType) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException{
        this.accountingPeriodService.loadIntoCache();
        this.aggregationService.loadIntoCache();
        this.cacheService.loadIntoCache();
        LocalDateTime startingTime = DateUtil.getDateTime();
        this.runId = System.currentTimeMillis();
        String key = this.dataService.getTenantId() + "TA" + this.runId;

        AggregationRequest aggregationRequest = AggregationRequest.builder()
                .isAggregationComplete(Boolean.FALSE)
                .isInprogress(Boolean.FALSE)
                .tenantId(this.dataService.getTenantId())
                .key(key).build();
        this.metricAggregationRequestService.save(aggregationRequest, AggregationRequestType.ATTRIBUTE_LEVEL_AGG);
        this.metricAggregationRequestService.save(aggregationRequest, AggregationRequestType.INSTRUMENT_LEVEL_AGG);
        this.metricAggregationRequestService.save(aggregationRequest, AggregationRequestType.METRIC_LEVEL_AGG);
        this.metricAggregationRequestService.save(aggregationRequest, AggregationRequestType.COMPLETE_AGG);
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("filePath", filePath)
                .addLong("run.id", this.runId)
                .addString("agg.key", key)
                .addString("tenantId", this.dataService.getTenantId())
                .toJobParameters();
        this.uploadData(jobLauncher
                ,job
                ,jobParameters
                ,runId
                ,startingTime
                ,filePath
                ,activityType
        );
    }
}

