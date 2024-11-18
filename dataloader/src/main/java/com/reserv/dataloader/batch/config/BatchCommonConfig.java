package com.reserv.dataloader.batch.config;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BatchCommonConfig {

    @Bean
    public JobExecutionDecider jobExecutionDecider() {
        // Decider to check if Job A completed successfully
        return (jobExecution, stepExecution) -> {
            // Check if jobA completed successfully and decide next step
            if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
                return FlowExecutionStatus.COMPLETED;  // Correct flow status
            }
            return FlowExecutionStatus.FAILED;  // If Job A fails, we stop the flow
        };
    }
}
