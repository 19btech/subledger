package com.reserv.dataloader.batch.config;

import com.fyntrac.common.component.InstrumentReplaySet;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.InstrumentReplayState;
import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.InstrumentReplayStateProcessor;
import com.reserv.dataloader.batch.reader.InstrumentReplayRecordReader;
import com.reserv.dataloader.batch.writer.GenericItemWriterAdapter;
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
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.data.builder.MongoItemWriterBuilder;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class InstrumentReplayStateConfig {

    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoTemplate mongoTemplate;
    private final InstrumentReplaySet instrumentReplaySet;

    public InstrumentReplayStateConfig(JobRepository jobRepository, MongoTemplate mongoTemplate,
                                     TenantDataSourceProvider dataSourceProvider,
                                     TenantContextHolder tenantContextHolder,
                                       InstrumentReplaySet instrumentReplaySet) {
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
        this.instrumentReplaySet = instrumentReplaySet;
    }

    @Bean("instrumentReplayStateJob")
    public Job instrumentReplayStateJob(JobCompletionNotificationListener listener, Step instrumentReplayStateStep) {
        return new JobBuilder("instrumentReplayStateJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(instrumentReplayStateStep)
                .end()
                .build();
    }

    @Bean
    public Step instrumentReplayStateStep() {
        return new StepBuilder("instrumentReplayStateStep", jobRepository)
                .<Records.InstrumentReplayRecord, InstrumentReplayState>chunk(10, new ResourcelessTransactionManager())
                .reader(instrumentReplayRecordReader("", 0L, this.instrumentReplaySet))
                .processor(instrumentReplayStateProcessor())
                .writer(instrumentReplayStateItemWriter(dataSourceProvider,
                        tenantContextHolder))
                .build();
    }

    @Bean
    @StepScope
    public InstrumentReplayRecordReader instrumentReplayRecordReader( @Value("#{jobParameters['tenantId']}") String tenantId,
                                                                      @Value("#{jobParameters['jobId']}") Long jobId,
                                                                      InstrumentReplaySet instrumentReplaySet) {
        return new InstrumentReplayRecordReader(tenantId, jobId, instrumentReplaySet);
    }
    @Bean
    public ItemProcessor<Records.InstrumentReplayRecord, InstrumentReplayState> instrumentReplayStateProcessor() {
        return new InstrumentReplayStateProcessor();
    }

    @Bean
    public ItemWriter<InstrumentReplayState> instrumentReplayStateItemWriter(TenantDataSourceProvider dataSourceProvider,
                                                          TenantContextHolder tenantContextHolder) {
        MongoItemWriter<InstrumentReplayState> delegate = new MongoItemWriterBuilder<InstrumentReplayState>()
                .template(mongoTemplate)
                .collection("InstrumentReplayState")
                .build();
        return new GenericItemWriterAdapter<>(delegate, dataSourceProvider, tenantContextHolder);
    }

}


