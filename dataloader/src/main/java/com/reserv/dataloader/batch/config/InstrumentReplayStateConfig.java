package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.tasklet.ReplayStateComputationTasklet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class InstrumentReplayStateConfig {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private ReplayStateComputationTasklet replayTasklet;

    @Bean("instrumentReplayStateJob")
    public Job instrumentReplayStateJob(PlatformTransactionManager transactionManager) {
        // CHANGE: Do NOT start with instrumentAttributeImportStep.
        // The data is already loaded. Just run the calculation.
        return new JobBuilder("instrumentReplayStateJob", jobRepository)
                .start(replayCalculationStep(transactionManager))
                .build();
    }

    @Bean
    public Step replayCalculationStep(PlatformTransactionManager transactionManager) {
        return new StepBuilder("replayCalculationStep", jobRepository)
                .tasklet(replayTasklet, transactionManager)
                .build();
    }
}