package com.reserv.dataloader.batch.config;

import com.fyntrac.common.component.InstrumentReplayQueue;
import com.fyntrac.common.component.TransactionActivityQueue;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.service.TransactionActivityReversalService;
import com.fyntrac.common.service.TransactionService;
import com.reserv.dataloader.batch.listener.ActivityReversalJobCompletionListener;
import com.reserv.dataloader.batch.processor.ReversalActivityProcessor;
import com.reserv.dataloader.batch.reader.InstrumentReplayQueueReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

@Configuration
@EnableBatchProcessing
@Slf4j
public class ActivityReversalBatchConfig {

    private final InstrumentReplayQueue instrumentReplayQueue;
    private final TransactionActivityReversalService reversalService;
    private final TransactionActivityQueue transactionActivityQueue;
    private final TransactionService transactionService;

    @Autowired
    public ActivityReversalBatchConfig(InstrumentReplayQueue instrumentReplayQueue,
                                       TransactionActivityReversalService reversalService,
                                       TransactionActivityQueue transactionActivityQueue,
                                       TransactionService transactionService) {
        this.instrumentReplayQueue = instrumentReplayQueue;
        this.reversalService = reversalService;
        this.transactionActivityQueue = transactionActivityQueue;
        this.transactionService = transactionService;
    }

    @Bean
    public Step reversalStep(JobRepository jobRepository,
                             PlatformTransactionManager transactionManager,
                             @Qualifier("reversalWriter") ItemWriter<List<TransactionActivity>> writer) {

        return new StepBuilder("reversalStep", jobRepository)
                .<Records.InstrumentReplayRecord, List<TransactionActivity>>chunk(1000, transactionManager)
                .reader(reversalInstrumentReplayReader("", 0L))
                .processor(reversalActivityProcessor("", 0L, reversalService, transactionActivityQueue, transactionService))
                .writer(writer)
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }



    @Bean
    @StepScope
    public ItemProcessor<Records.InstrumentReplayRecord, List<TransactionActivity>> reversalActivityProcessor(
            @Value("#{jobParameters['tenantId']}") String tenantId,
            @Value("#{jobParameters['jobId']}") Long jobId, TransactionActivityReversalService reversalService
            , TransactionActivityQueue transactionActivityQueue
    , TransactionService transactionService) {
        return new ReversalActivityProcessor(tenantId, jobId, reversalService, transactionActivityQueue, transactionService);
    }

    @Bean("reversalJob")
    public Job reversalJob(JobRepository jobRepository,
                           @Qualifier("reversalStep") Step reversalStep,
                           ActivityReversalJobCompletionListener jobCompletionListener) {

        return new JobBuilder("reversalJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(reversalStep)
                .listener(jobCompletionListener)
                .build();
    }

    @Bean
    @StepScope
    @Qualifier("reversalInstrumentReplayReader")
    public InstrumentReplayQueueReader reversalInstrumentReplayReader(
            @Value("#{jobParameters['tenantId']}") String tenantId,
            @Value("#{jobParameters['jobId']}") Long jobId) {

        return new InstrumentReplayQueueReader(tenantId, jobId, instrumentReplayQueue);
    }
}
