package com.reserv.dataloader.batch.listener;

import com.reserv.dataloader.pulsar.producer.ReclassMessagProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.utils.Key;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.fyntrac.common.dto.record.RecordFactory;

@Component
@Slf4j
public class InstrumentAttributeJobCompletionListener implements JobExecutionListener {

    @Autowired
    com.fyntrac.common.repository.MemcachedRepository memcachedRepository;

    @Autowired
    com.fyntrac.common.service.DataService<com.fyntrac.common.entity.Settings> dataService;

    @Autowired
    ReclassMessagProducer reclassMessagProducer;
    @Override
    public void beforeJob(JobExecution jobExecution) {
        // No-op
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        StringBuilder jobExecutionMessage = new StringBuilder(0);
        String lineSeparator = System.lineSeparator();
        String tenantId = jobExecution.getJobParameters().getString("tenantId");

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
                    log.info("TransactionAttribute file uploaded successfully");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    String dataKey= Key.reclassMessageList(tenantId);
                    Records.ReclassMessageRecord reclassMessageRecord = RecordFactory.createReclassMessageRecord(tenantId, dataKey);
                    reclassMessagProducer.sendReclassMessage(reclassMessageRecord);}
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
}
