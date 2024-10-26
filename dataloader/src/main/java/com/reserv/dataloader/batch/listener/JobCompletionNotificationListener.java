package com.reserv.dataloader.batch.listener;

import com.fyntrac.common.repository.ActivityLogRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    @Autowired
    private ActivityLogRepo activityLogRepo;

    @Override
    public void afterJob(JobExecution jobExecution) {
        StringBuilder jobExecutionMessage = new StringBuilder(0);
        String lineSeparator = System.getProperty("line.separator");

        jobExecutionMessage.append("Job Status : " + jobExecution.getStatus()).append(lineSeparator)
                .append("Job Name : " + jobExecution.getJobInstance().getJobName()).append(lineSeparator)
                .append("Job Start Time : " + jobExecution.getStartTime()).append(lineSeparator)
                .append("Job End Time : " + jobExecution.getEndTime()).append(lineSeparator)
                .append("Upload File Name : " + jobExecution.getJobParameters().getString("fileName")).append(lineSeparator)
                .append("Step execution details : " + jobExecution.getStepExecutions().toString());

        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("jobExecutionMessage");
        }else if(jobExecution.getStatus() == BatchStatus.FAILED) {
            log.error("jobExecutionMessage");
        }

//        try {
//           //do to
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
    }
}

