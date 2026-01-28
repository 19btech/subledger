package com.reserv.dataloader.service.upload;

import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.enums.*;
import com.fyntrac.common.service.*;
import com.reserv.dataloader.entity.ActivityLog;
import com.reserv.dataloader.service.CacheService;
import com.fyntrac.common.service.aggregation.AggregationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.utils.DateUtil;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import com.fyntrac.common.entity.Batch;

@Service
@Slf4j
public class ActivityUploadService {

    @Autowired
    private Job instrumentAttributeUploadJob;

    @Autowired
    private Job transactionActivityUploadJob;

    @Autowired
    protected JobLauncher jobLauncher;

    @Autowired
    AttributeService attributeService;

    @Autowired
    protected  DataService dataService;

    @Autowired
    Job dynamicLoadJob;

    private final AccountingPeriodDataUploadService accountingPeriodService;
    private final AggregationService aggregationService;
    private final TransactionActivityService transactionActivityService;
    private final CacheService cacheService;
    private final ExecutionStateService executionStateService;

    @Autowired
    public ActivityUploadService(AccountingPeriodDataUploadService accountingPeriodService
            ,TransactionActivityService transactionActivityService
            , AggregationService aggregationService
            , CacheService cacheService,
                                 ExecutionStateService executionStateService) {
        this.accountingPeriodService = accountingPeriodService;
        this.aggregationService = aggregationService;
        this.transactionActivityService = transactionActivityService;
        this.cacheService = cacheService;
        this.executionStateService = executionStateService;
    }

    public void uploadCustomTableData(Map<CustomTableDefinition, String> dataMap) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {

        for(Map.Entry<CustomTableDefinition, String> entry :  dataMap.entrySet()) {
            CustomTableDefinition customTableDefinition = entry.getKey();
            String fileName = entry.getValue();
            long uploadId = Long.parseLong(
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"))
            );
            JobParameters params = new JobParametersBuilder()
                    .addString("tableDefId", customTableDefinition.getId()) // ID from Mongo
                    .addString("filePath", fileName)
                    .addLong("time", System.currentTimeMillis())
                    .addLong("run.id", uploadId)
                    .toJobParameters();
            JobExecution jobExecution = null;

            try {
                jobExecution = jobLauncher.run(dynamicLoadJob, params);
            }catch (Exception exp){
                throw exp;
            }finally {
                if(jobExecution != null && jobExecution.getStatus() == BatchStatus.COMPLETED) {

                    ExecutionState executionState = this.executionStateService.getExecutionState();

                    Integer postingDate = null;

                    if(customTableDefinition.getTableType() == CustomTableType.OPERATIONAL) {
                        Integer executionDate = executionState.getExecutionDate();
                        Integer maxPostingDate =
                                this.dataService.getMaxPostingDate(customTableDefinition.getTableName(), executionDate);
                        postingDate = maxPostingDate > executionDate ? maxPostingDate : executionDate;

                    }
                    this.logActivity(uploadId, postingDate, customTableDefinition.getTableName(),jobExecution,
                            FileUploadActivityType.CUSTOM_TABLE);
                }
            }
        }

    }

    public void uploadActivity(long uploadId, Map<AccountingRules, String> activityMap) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, ExecutionException, InterruptedException, JobRestartException {
        FileUploadActivityType activityType = FileUploadActivityType.INSTRUMENT_ATTRIBUTE;
        String instrumentAttributeFilePath = activityMap.get(AccountingRules.INSTRUMENTATTRIBUTE);
        String transactionActivityFilePath = activityMap.get(AccountingRules.TRANSACTIONACTIVITY);
        int instrumentAttributeActivity = (instrumentAttributeFilePath!= null && !instrumentAttributeFilePath.isEmpty()) ? 1 : 0;
        int transactionActivity =  (transactionActivityFilePath!= null && !transactionActivityFilePath.isEmpty()) ? 1 : 0;

        Long runid = System.currentTimeMillis();
        long jobId = runid;
        try {
            Batch activityBatch = this.createBatch();
            if(instrumentAttributeFilePath !=null) {
                JobParameters instrumentAttributeJobParameter =
                        createInstrumentAttributeJob(uploadId, instrumentAttributeFilePath, activityBatch,
                                (instrumentAttributeActivity + transactionActivity), jobId);
                JobExecution instrumentAttributeExecution = jobLauncher.run(instrumentAttributeUploadJob, instrumentAttributeJobParameter);
                runid = instrumentAttributeJobParameter.getLong("run.id");

                if (instrumentAttributeExecution.getStatus() == BatchStatus.COMPLETED) {
                    // Job A was successful, now launch Job B
                    log.info("Job instrumentAttributeUploadJob completed successfully. Starting Job transactionActivityUploadJob.");
                    if(transactionActivityFilePath != null) {
                        JobParameters transactionActivityJobParameter =
                                createTransactionActivityJob(uploadId, transactionActivityFilePath, activityBatch,
                                        (instrumentAttributeActivity + transactionActivity), jobId);


                        runid = transactionActivityJobParameter.getLong("run.id");
                        JobExecution transactionActivityExecution = jobLauncher.run(transactionActivityUploadJob, transactionActivityJobParameter);

                    }
                } else {
                    // If Job instrumentAttributeUploadJob fails, handle failure and don't run Job transactionActivityExecution
                    log.info("Job instrumentAttributeUploadJob failed. Job transactionActivityExecution will not be executed.");
                }
            }else{
                JobParameters transactionActivityJobParameter =
                        createTransactionActivityJob(uploadId, transactionActivityFilePath, activityBatch,
                                (instrumentAttributeActivity + transactionActivity), jobId);
                runid = transactionActivityJobParameter.getLong("run.id");
                JobExecution transactionActivityExecution = jobLauncher.run(transactionActivityUploadJob, transactionActivityJobParameter);

            }
        }catch (Exception e ){
            String stackTrace = com.fyntrac.common.utils.StringUtil.getStackTrace(e);
            log.error(stackTrace);
            logActivityException(runid, DateUtil.getDateTime(), transactionActivityFilePath, activityType);
            throw new ExecutionException(e);
        }
    }

    public JobParameters createInstrumentAttributeJob(long uploadId, String filePath, Batch activityBatch, long activityCount, long jobId) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException, ExecutionException, InterruptedException {
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
        return  new JobParametersBuilder()
                .addString("filePath", filePath)
                .addString("columnName", columnNames.toString())
                .addLong("run.id", runid)
                .addString("tenantId", this.dataService.getTenantId())
                .addLong("batchId", activityBatch.getId())
                .addLong("activityCount", activityCount)
                .addLong("jobId", jobId)
                .addLong("uploadId", uploadId)
                .toJobParameters();
    }

    public JobParameters createTransactionActivityJob(long uploadId, String filePath, Batch activityBatch, long activityCount, long jobId) throws ExecutionException, InterruptedException {
        this.accountingPeriodService.loadIntoCache();
        this.aggregationService.loadIntoCache();
        this.cacheService.loadIntoCache();
        LocalDateTime startingTime = DateUtil.getDateTime();
        long runId = System.currentTimeMillis();
        String key = this.dataService.getTenantId() + "TA" + runId;

        StringBuilder columnNames = new StringBuilder();
        columnNames.append("ACTIVITYUPLOADID:NUMBER");
        columnNames.append(",POSTINGDATE:DATE");
        columnNames.append(",AMOUNT:NUMBER");
        columnNames.append(",TRANSACTIONDATE:DATE");
        columnNames.append(",TRANSACTIONNAME:STRING");
        columnNames.append(",ATTRIBUTEID:STRING");
        columnNames.append(",INSTRUMENTID:STRING");

        return new JobParametersBuilder()
                .addString("filePath", filePath)
                .addString("activityColumnName", columnNames.toString())
                .addLong("run.id", runId)
                .addString(com.fyntrac.common.utils.Key.aggregationKey(this.dataService.getTenantId(), runId), key)
                .addLong("batchId", activityBatch.getId())
                .addString("tenantId", this.dataService.getTenantId())
                .addLong("activityCount", activityCount)
                .addLong("jobId", jobId)
                .addLong("uploadId", uploadId)
                .toJobParameters();
    }

    private void logActivity(long uploadId,Integer postingDate, String tableName, JobExecution execution,
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
                    .uploadId(uploadId)
                    .jobId(execution.getJobParameters().getLong("run.id"))
                    .uploadFilePath(execution.getJobParameters().getString("filePath"))

                    // 4. Improved Status Logic (Checks Step status specifically)
                    .activityStatus(!stepExecution.getFailureExceptions().isEmpty() ? "FAILED" : stepExecution.getExitStatus().getExitCode())

                    // 5. CRITICAL: Don't save the 'stepExecution' object. Save the stats instead.
                    .recordsRead(stepExecution.getReadCount())
                    .recordsWritten(stepExecution.getWriteCount())
                    .recordsSkipped(stepExecution.getSkipCount())
                    .postingDate(postingDate)
                    // 6. Capture Error Message if exists
                    .errorMessage(stepExecution.getFailureExceptions().isEmpty() ? null : stepExecution.getFailureExceptions().get(0).getMessage())

                    .build();

            logs.add(activityLog);
        }


        dataService.saveAll(logs, ActivityLog.class);

    }


    private void logActivityException(Long runId, LocalDateTime startingtime, String filePath, FileUploadActivityType activityType) {
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

    private Batch createBatch() {
        long batchId = this.dataService.generateSequence(SequenceNames.BATCHID.name());
        Batch activityBatch = Batch.builder().id(batchId).batchStatus(com.fyntrac.common.enums.BatchStatus.PENDING)
                .batchType(BatchType.ACTIVITY).uploadDate(new Date()).build();
        this.dataService.save(activityBatch);
         return activityBatch;
    }
}
