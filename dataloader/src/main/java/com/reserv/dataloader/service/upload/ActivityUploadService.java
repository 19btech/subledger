package com.reserv.dataloader.service.upload;

import com.fyntrac.common.enums.AccountingRules;
import com.fyntrac.common.enums.FileUploadActivityType;
import com.reserv.dataloader.service.AccountingPeriodService;
import com.reserv.dataloader.service.AggregationRequestService;
import com.reserv.dataloader.service.CacheService;
import com.reserv.dataloader.service.TransactionActivityService;
import com.reserv.dataloader.service.aggregation.AggregationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fyntrac.common.service.AttributeService;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.enums.AggregationRequestType;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class ActivityUploadService {

    @Autowired
    private Job instrumentAttributeUploadJob;

    @Autowired
    private Job transactionActivityUploadJob;

    @Autowired
    AggregationRequestService metricAggregationRequestService;

    @Autowired
    protected JobLauncher jobLauncher;

    @Autowired
    AttributeService attributeService;

    @Autowired
    DataService dataService;

    private final AccountingPeriodService accountingPeriodService;
    private final AggregationService aggregationService;
    private final TransactionActivityService transactionActivityService;
    private final CacheService cacheService;


    @Autowired
    public ActivityUploadService(AccountingPeriodService accountingPeriodService
            ,TransactionActivityService transactionActivityService
            , AggregationService aggregationService
            , CacheService cacheService) {
        this.accountingPeriodService = accountingPeriodService;
        this.aggregationService = aggregationService;
        this.transactionActivityService = transactionActivityService;
        this.cacheService = cacheService;
    }

    public void uploadActivity(Map<AccountingRules, String> activityMap) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, ExecutionException, InterruptedException, JobRestartException {
        FileUploadActivityType activityType = FileUploadActivityType.INSTRUMENT_ATTRIBUTE;
        String filePath = activityMap.get(AccountingRules.INSTRUMENTATTRIBUTE);
        Long runid = System.currentTimeMillis();
        try {
            JobParameters instrumentAttributeJobParameter = createInstrumentAttributeJob(filePath);
            filePath = activityMap.get(AccountingRules.TRANSACTIONACTIVITY);
            JobParameters transactionActivityJobParameter = createTransactionActivityJob(filePath);

            JobExecution instrumentAttributeExecution = jobLauncher.run(instrumentAttributeUploadJob, instrumentAttributeJobParameter);
            runid = instrumentAttributeJobParameter.getLong("run.id");
            this.logActivity(instrumentAttributeExecution, FileUploadActivityType.INSTRUMENT_ATTRIBUTE);
            if (instrumentAttributeExecution.getStatus() == BatchStatus.COMPLETED) {
                // Job A was successful, now launch Job B
                log.info("Job instrumentAttributeUploadJob completed successfully. Starting Job transactionActivityUploadJob.");
                runid = transactionActivityJobParameter.getLong("run.id");
                JobExecution transactionActivityExecution = jobLauncher.run(transactionActivityUploadJob, transactionActivityJobParameter);
                this.logActivity(transactionActivityExecution, FileUploadActivityType.TRANSACTION_ACTIVITY);
            } else {
                // If Job instrumentAttributeUploadJob fails, handle failure and don't run Job transactionActivityExecution
                log.info("Job instrumentAttributeUploadJob failed. Job transactionActivityExecution will not be executed.");
            }
        }catch (Exception e ){
            log.error(e.getMessage());
            log.error(e.getCause().getMessage());
            logActivityException(runid, DateUtil.getDateTime(), filePath, activityType);
        }
    }

    public JobParameters createInstrumentAttributeJob(String filePath) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException, ExecutionException, InterruptedException {
        LocalDateTime startingTime = DateUtil.getDateTime();
        long runid = System.currentTimeMillis();
        StringBuilder columnNames = new StringBuilder();

        columnNames.append("ACTIVITYUPLOADID:NUMBER");
        columnNames.append(",EFFECTIVEDATE:DATE");
        columnNames.append(",ATTRIBUTEID:STRING");
        columnNames.append(",INSTRUMENTID:STRING");

        Collection<Attributes> attributes = attributeService.getAllAttributes();
        for(Attributes attribute : attributes) {
            columnNames.append(",");
            columnNames.append(attribute.getAttributeName()).append(":").append(attribute.getDataType());
        }
        accountingPeriodService.loadIntoCache();
        return  new JobParametersBuilder()
                .addString("filePath", filePath)
                .addString("columnName", columnNames.toString())
                .addLong("run.id", runid)
                .addString("tenantId", this.dataService.getTenantId())
                .toJobParameters();
    }

    public JobParameters createTransactionActivityJob(String filePath) throws ExecutionException, InterruptedException {
        this.accountingPeriodService.loadIntoCache();
        this.aggregationService.loadIntoCache();
        this.cacheService.loadIntoCache();
        LocalDateTime startingTime = DateUtil.getDateTime();
        long runId = System.currentTimeMillis();
        String key = this.dataService.getTenantId() + "TA" + runId;

        com.fyntrac.common.entity.AggregationRequest aggregationRequest = com.fyntrac.common.entity.AggregationRequest.builder()
                .isAggregationComplete(Boolean.FALSE)
                .isInprogress(Boolean.FALSE)
                .tenantId(this.dataService.getTenantId())
                .key(key).build();
        this.metricAggregationRequestService.save(aggregationRequest, AggregationRequestType.ATTRIBUTE_LEVEL_AGG);
        this.metricAggregationRequestService.save(aggregationRequest, AggregationRequestType.INSTRUMENT_LEVEL_AGG);
        this.metricAggregationRequestService.save(aggregationRequest, AggregationRequestType.METRIC_LEVEL_AGG);
        this.metricAggregationRequestService.save(aggregationRequest, AggregationRequestType.COMPLETE_AGG);
        return new JobParametersBuilder()
                .addString("filePath", filePath)
                .addLong("run.id", runId)
                .addString(com.fyntrac.common.utils.Key.aggregationKey(), key)
                .addString("tenantId", this.dataService.getTenantId())
                .toJobParameters();
    }

    private void logActivity(JobExecution execution, com.fyntrac.common.enums.FileUploadActivityType activityType) {
        com.fyntrac.common.entity.ActivityLog activityLog = com.fyntrac.common.entity.ActivityLog.builder().
                activityType(activityType)
                .jobName(execution.getJobInstance().getJobName())
                .startingTime(execution.getStartTime())
                .endingTime(execution.getEndTime())
                .jobId(execution.getJobParameters().getLong("run.id"))
                .uploadFilePath(execution.getJobParameters().getString("filePath"))
                .activityStatus(execution.getExitStatus().getExitCode())
                .build();
        dataService.save(activityLog);

    }

    private void logActivityException(Long runId, LocalDateTime startingtime, String filePath, FileUploadActivityType activityType) {
        LocalDateTime endingTime = DateUtil.getDateTime();
        com.fyntrac.common.entity.ActivityLog activityLog = com.fyntrac.common.entity.ActivityLog.builder().
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
