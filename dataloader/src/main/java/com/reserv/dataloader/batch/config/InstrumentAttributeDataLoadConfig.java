package com.reserv.dataloader.batch.config;

import com.fyntrac.common.component.InstrumentReplayQueue;
import com.fyntrac.common.component.InstrumentReplaySet;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.InstrumentAttributeService;
import com.reserv.dataloader.batch.listener.InstrumentAttributeJobCompletionListener;
import com.reserv.dataloader.batch.processor.InstrumentAttributeItemProcessor;
import com.reserv.dataloader.batch.writer.InstrumentAttributeWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.data.builder.MongoItemWriterBuilder;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.IOException;
import java.util.Map;

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class InstrumentAttributeDataLoadConfig {
    private final JobRepository jobRepository;
    private final TenantDataSourceProvider dataSourceProvider;
    private MongoTemplate mongoTemplate;
    private MemcachedRepository memcachedRepository;
    private InstrumentAttributeService instrumentAttributeService;
    private AccountingPeriodService accountingPeriodService;
    private ExecutionStateService executionStateService;
    private final BatchCommonConfig batchCommonConfig;


    public InstrumentAttributeDataLoadConfig(JobRepository jobRepository, MongoTemplate mongoTemplate,
                                             TenantDataSourceProvider dataSourceProvider,
                                             MemcachedRepository memcachedRepository,
                                             InstrumentAttributeService instrumentAttributeService,
                                             AccountingPeriodService accountingPeriodService,
                                             ExecutionStateService executionStateService,
                                             BatchCommonConfig batchCommonConfig) {
        this.jobRepository = jobRepository;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
        this.memcachedRepository = memcachedRepository;
        this.instrumentAttributeService = instrumentAttributeService;
        this.accountingPeriodService = accountingPeriodService;
        this.executionStateService = executionStateService;
        this.batchCommonConfig = batchCommonConfig;
    }

    @Bean("instrumentAttributeUploadJob")
    public Job instrumentAttributeUploadJob(InstrumentAttributeJobCompletionListener listener, Step instrumentAttributeImportStep) {
        return new JobBuilder("instrumentAttributeUploadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(instrumentAttributeImportStep)
                .end()
                .build();
    }

    @Bean
    public Step instrumentAttributeImportStep() throws IOException {
        return new StepBuilder("instrumentAttributeImportStep", jobRepository)
                .<Map<String,Object>,InstrumentAttribute>chunk(10, new ResourcelessTransactionManager())
                .reader(this.batchCommonConfig.genericReader(""))
                .processor(instrumentAttributeMapItemProcessor())
                .taskExecutor(new SimpleAsyncTaskExecutor("instrumentAttributeImportStep"))
                .writer(instrumentAttributeWriter(dataSourceProvider
                        , this.memcachedRepository
                        , this.instrumentAttributeService
                        , this.accountingPeriodService
                        , this.executionStateService))
                .build();
    }

    @Bean
    public ItemProcessor<Map<String,Object>,InstrumentAttribute> instrumentAttributeMapItemProcessor() {
        return new InstrumentAttributeItemProcessor();
    }

    @Bean
    public ItemWriter<InstrumentAttribute> instrumentAttributeWriter(TenantDataSourceProvider dataSourceProvider,
                                                                     MemcachedRepository memcachedRepository,
                                                                     InstrumentAttributeService instrumentAttributeService,
                                                                     AccountingPeriodService accountingPeriodService,
                                                                     ExecutionStateService executionStateService
    ) {
        MongoItemWriter<InstrumentAttribute> delegate = new MongoItemWriterBuilder<InstrumentAttribute>()
                .template(mongoTemplate)
                .collection("InstrumentAttribute")
                .build();

        return new InstrumentAttributeWriter(delegate, dataSourceProvider,  memcachedRepository,
                instrumentAttributeService, accountingPeriodService, executionStateService);
    }
}