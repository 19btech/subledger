package com.reserv.dataloader.batch.listener;

import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.enums.ActivityType;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.InstrumentAttributeService;
import com.reserv.dataloader.pulsar.producer.ReclassMessagProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.utils.Key;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.fyntrac.common.dto.record.RecordFactory;

import java.util.NoSuchElementException;

@Component
@Slf4j
public class InstrumentAttributeJobCompletionListener implements JobExecutionListener {

    @Autowired
    ReclassMessagProducer reclassMessagProducer;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    Job reversalJob;

    @Autowired
    Job instrumentReplayStateJob;

    private final ExecutionStateService executionStateService;
    private final InstrumentAttributeService instrumentAttributeService;
    @Autowired
    public InstrumentAttributeJobCompletionListener(ExecutionStateService executionStateService
    , InstrumentAttributeService instrumentAttributeService) {
        this.executionStateService = executionStateService;
        this.instrumentAttributeService = instrumentAttributeService;
    }
    @Override
    public void beforeJob(JobExecution jobExecution) {
        // No-op
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        StringBuilder jobExecutionMessage = new StringBuilder(0);
        String lineSeparator = System.lineSeparator();
        String tenantId = jobExecution.getJobParameters().getString("tenantId");
        Long runId = jobExecution.getJobParameters().getLong("run.id");
        Long activityCount = jobExecution.getJobParameters().getLong("activityCount");
        Long jobId = jobExecution.getJobParameters().getLong("jobId");

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
                try{
                    if(activityCount != null && activityCount == 1) {
                        assert tenantId != null;
                        assert jobId != null;
                        JobParameters jobParameters = new JobParametersBuilder()
                                .addString("tenantId", tenantId)
                                .addLong("jobId", jobId)
                                .addLong("key", runId)
                                .toJobParameters();

                        jobLauncher.run(reversalJob, jobParameters);
                        jobLauncher.run(instrumentReplayStateJob, jobParameters);
                    }
                }catch (Exception e){
                    throw new RuntimeException(e);
                }finally {

                    String dataKey= Key.reclassMessageList(tenantId, runId);
                    Records.ReclassMessageRecord reclassMessageRecord = RecordFactory.createReclassMessageRecord(tenantId, dataKey);
                    reclassMessagProducer.sendReclassMessage(reclassMessageRecord);}
                ExecutionState executionState = updateExecutionState(tenantId);

                JobParameters jobParameters = new JobParametersBuilder()
                        .addLong("execution-date", (long) executionState.getExecutionDate())
                        .addString("activity-type", ActivityType.INSTRUMENT_ATTRIBUTE.toString())
                        .toJobParameters();

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

    private ExecutionState updateExecutionState(String tenantId) {
        // Attempt to update the last accounting period of transaction activity upload
        try {
            // Retrieve the execution state directly (assuming it returns null if not found)
            ExecutionState executionState = this.executionStateService.getExecutionState();

            // Check if the execution state is not null
            if (executionState != null) {
                // Retrieve the latest activity posting date for the given tenant
                int latestPostingDate = this.instrumentAttributeService.getLatestActivityPostingDate(tenantId);

                // Update the activity posting date in the execution state
                if (latestPostingDate > executionState.getExecutionDate()) {
                    executionState.setLastExecutionDate(executionState.getExecutionDate());
                    executionState.setExecutionDate(latestPostingDate);
                    // Persist the updated execution state
                    return this.executionStateService.update(executionState);
                }
            } else {
                // Retrieve the latest activity posting date for the given tenant
                int latestPostingDate = this.instrumentAttributeService.getLatestActivityPostingDate(tenantId);

                // Create a new execution state since none was found
                ExecutionState executionStateNew = ExecutionState.builder()
                        .executionDate(latestPostingDate)
                        .lastExecutionDate(0)
                        .build();

                // Persist the new execution state
                log.warn("No execution state found for updating the last transaction activity posting date.");
                return this.executionStateService.update(executionStateNew);
            }
            return executionState;
        } catch (NoSuchElementException e) {
            log.error("Execution state not found when trying to update the last transaction activity upload reporting period.", e);
        } catch (Exception e) {
            log.error("Failed to update settings with the last transaction activity upload reporting period for tenantId: {}", tenantId, e);
        }

        return null;
    }
}
