package com.reserv.dataloader.batch.listener;

import com.fyntrac.common.dto.record.Records;
import com.reserv.dataloader.pulsar.producer.GeneralLedgerMessageProducer;
import com.fyntrac.common.repository.MemcachedRepository;
import com.reserv.dataloader.service.AggregationRequestService;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.TransactionActivityService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.service.SettingsService;

@Component
@Slf4j
public class TransactionActivityJobCompletionListener implements JobExecutionListener {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private Job instrumentAggregationJob;

    @Autowired
    private Job attributeAggregationJob;

    @Autowired
    private Job metricAggregationJob;

    @Autowired
    AggregationRequestService aggregationRequestService;

    @Autowired
    MemcachedRepository memcachedRepository;

    @Autowired
    TransactionActivityService transactionActivityService;

    @Autowired
    SettingsService settingsService;

    @Autowired
    DataService<com.fyntrac.common.entity.Settings> dataService;

    @Autowired
    GeneralLedgerMessageProducer generalLedgerMessageProducer;
    @Override
    public void beforeJob(JobExecution jobExecution) {
        // No-op
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        StringBuilder jobExecutionMessage = new StringBuilder(0);
        String lineSeparator = System.lineSeparator();
        String tenantId = jobExecution.getJobParameters().getString("tenantId");
        String key = jobExecution.getJobParameters().getString(com.fyntrac.common.utils.Key.aggregationKey());
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            jobExecutionMessage.append("Job Status : ")
                    .append(jobExecution.getStatus())
                    .append(lineSeparator).append("Job Name : ")
                    .append(jobExecution.getJobInstance().getJobName())
                    .append(lineSeparator).append("Job Start Time : ")
                    .append(jobExecution.getStartTime())
                    .append(lineSeparator).append("Job End Time : ")
                    .append(jobExecution.getEndTime())
                    .append(lineSeparator)
                    .append("Upload File Name : ").
                    append(jobExecution.getJobParameters().getString("fileName"))
                    .append(lineSeparator)
                    .append("Step execution details : ")
                    .append(jobExecution.getStepExecutions().toString());

            if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
                log.info(jobExecutionMessage.toString());
                Long runId = System.currentTimeMillis();
                JobParameters jobParameters = new JobParametersBuilder()
                        .addLong("run.id", runId)
                        .toJobParameters();
                try {
                    jobLauncher.run(attributeAggregationJob, jobParameters);
                    jobLauncher.run(instrumentAggregationJob, jobParameters);
                    jobLauncher.run(metricAggregationJob, jobParameters);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    updatelastTransactionActivityUploadReportingPeriod(tenantId);
                    Records.GeneralLedgerMessageRecord glRec = RecordFactory.createGeneralLedgerMessageRecord(tenantId, key);
                    generalLedgerMessageProducer.bookTempGL(glRec);

                   // memcachedRepository.delete(aggregationRequest.getKey());
                }
            }

        }else if(jobExecution.getStatus() == BatchStatus.FAILED) {
            jobExecutionMessage.append("Job Status : ")
                    .append(jobExecution.getStatus())
                    .append(lineSeparator).append("Job Name : ")
                    .append(jobExecution.getJobInstance().getJobName())
                    .append(lineSeparator).append("Job Start Time : ")
                    .append(jobExecution.getStartTime())
                    .append(lineSeparator).append("Job End Time : ")
                    .append(jobExecution.getEndTime())
                    .append(lineSeparator)
                    .append("Upload File Name : ").
                    append(jobExecution.getJobParameters().getString("fileName"))
                    .append(lineSeparator)
                    .append("Step execution details : ")
                    .append(jobExecution.getStepExecutions().toString());

            log.error(jobExecutionMessage.toString());
        }
    }

    private void updatelastTransactionActivityUploadReportingPeriod(String tenantId){
        // Set last accounting period of transaction activity upload
        try {
            com.fyntrac.common.entity.Settings settings = this.settingsService.fetch(tenantId);
            int lastAccountingPeriod = this.transactionActivityService.getLastAccountingPeriodId(tenantId);
            settings.setLastTransactionActivityUploadReportingPeriod(lastAccountingPeriod);
            this.dataService.saveObject(settings, tenantId);
        } catch (Exception e) {
            log.error("Failed to update settings with the last transaction activity upload reporting period", e);
        }
    }
}
