package com.reserv.dataloader.batch.config;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.repository.AttributesRepository;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import com.reserv.dataloader.service.ActivityValidationLogService;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.InstrumentAttributeService;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.batch.listener.InstrumentAttributeJobCompletionListener;
import com.reserv.dataloader.batch.listener.ValidationLoggingListener;
import com.reserv.dataloader.batch.processor.InstrumentAttributeItemProcessor;
import com.reserv.dataloader.batch.writer.InstrumentAttributeWriter;
import com.reserv.dataloader.validation.InstrumentAttributeValidator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.IOException;
import java.util.Map;

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class InstrumentAttributeDataLoadConfig {

    private final JobRepository jobRepository;
    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoTemplate mongoTemplate;
    private final MemcachedRepository memcachedRepository;
    private final InstrumentAttributeService instrumentAttributeService;
    private final AccountingPeriodService accountingPeriodService;
    private final ExecutionStateService executionStateService;
    private final BatchCommonConfig batchCommonConfig;

    public InstrumentAttributeDataLoadConfig(JobRepository jobRepository,
                                             MongoTemplate mongoTemplate,
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

    // ------------------------------------------------------------------
    // Job
    // ------------------------------------------------------------------

    @Bean("instrumentAttributeUploadJob")
    public Job instrumentAttributeUploadJob(
            InstrumentAttributeJobCompletionListener listener,
            Step instrumentAttributeImportStep) {
        return new JobBuilder("instrumentAttributeUploadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(instrumentAttributeImportStep)
                .end()
                .build();
    }

    // ------------------------------------------------------------------
    // Step  — follows AggregationDataLoadConfig.aggregationImportStep()
    // ------------------------------------------------------------------

    @Bean
    public Step instrumentAttributeImportStep(
            ItemProcessor<Map<String, Object>, InstrumentAttribute> instrumentAttributeItemProcessor,
            ValidationLoggingListener validationLoggingListener,
            AttributesRepository attributesRepository) throws IOException {
        return new StepBuilder("instrumentAttributeImportStep", jobRepository)
                .<Map<String, Object>, InstrumentAttribute>chunk(10, new ResourcelessTransactionManager())
                .reader(this.batchCommonConfig.genericReader(""))
                .processor(instrumentAttributeItemProcessor)
                .faultTolerant()
                .skip(ItemValidationException.class)
                .skipLimit(Integer.MAX_VALUE)
                .listener((StepExecutionListener) validationLoggingListener)
                .listener((ItemProcessListener) validationLoggingListener)
                .listener(instrumentAttributeItemProcessor)     // @BeforeStep wiring
                .writer(instrumentAttributeWriter(
                        dataSourceProvider,
                        memcachedRepository,
                        instrumentAttributeService,
                        accountingPeriodService,
                        executionStateService,
                        attributesRepository))
                .build();
    }

    // ------------------------------------------------------------------
    // Processor Bean  — @StepScope matches AggregationDataLoadConfig pattern
    // ------------------------------------------------------------------

    @Bean
    @StepScope
    public InstrumentAttributeItemProcessor instrumentAttributeItemProcessor(
            InstrumentAttributeValidator validator,
            ActivityValidationLogService validationLogService,
            AttributesRepository attributesRepository) {
        return new InstrumentAttributeItemProcessor(validator, validationLogService, attributesRepository);
    }

    // ------------------------------------------------------------------
    // Writer
    // ------------------------------------------------------------------

    @Bean
    public ItemWriter<InstrumentAttribute> instrumentAttributeWriter(
            TenantDataSourceProvider dataSourceProvider,
            MemcachedRepository memcachedRepository,
            InstrumentAttributeService instrumentAttributeService,
            AccountingPeriodService accountingPeriodService,
            ExecutionStateService executionStateService,
            AttributesRepository attributesRepository) {

        MongoItemWriter<InstrumentAttribute> delegate = new MongoItemWriterBuilder<InstrumentAttribute>()
                .template(mongoTemplate)
                .collection("InstrumentAttribute")
                .build();

        return new InstrumentAttributeWriter(delegate, dataSourceProvider, memcachedRepository,
                instrumentAttributeService, accountingPeriodService, executionStateService, attributesRepository);
    }
}