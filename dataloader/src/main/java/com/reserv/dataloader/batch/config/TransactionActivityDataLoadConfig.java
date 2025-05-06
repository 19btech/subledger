package com.reserv.dataloader.batch.config;

import com.fyntrac.common.service.*;
import com.fyntrac.common.utils.DateUtil;
import com.reserv.dataloader.batch.listener.TransactionActivityJobCompletionListener;
import com.reserv.dataloader.batch.processor.TransactionActivityItemProcessor;
import com.reserv.dataloader.batch.tasklet.AttributeLevelAggregatorTasklet;
import com.reserv.dataloader.batch.tasklet.InstrumentActivityStateTasklet;
import com.reserv.dataloader.batch.tasklet.InstrumentLevelAggregatorTasklet;
import com.reserv.dataloader.batch.tasklet.MetricLevelAggregatorTasklet;
import com.reserv.dataloader.batch.writer.TransactionActivityItemWriter;
import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.MetricLevelLtd;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.repository.MemcachedRepository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.data.builder.MongoItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.validation.BindException;

import java.util.Date;

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class TransactionActivityDataLoadConfig {

    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoTemplate mongoTemplate;
    private final MemcachedRepository memcachedRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataService<MetricLevelLtd> dataService;
    private  final SettingsService settingsService;
    private final InstrumentAttributeService instrumentAttributeService;
    private final AttributeService attributeService;
    private final AccountingPeriodService accountingPeriodService;
    private final ExecutionStateService executionStateService;
    private final TransactionActivityService activityService;

    @Autowired
    public TransactionActivityDataLoadConfig(JobRepository jobRepository
                                     , MongoTemplate mongoTemplate
                                     , TenantDataSourceProvider dataSourceProvider
                                     , TenantContextHolder tenantContextHolder
                                     , MemcachedRepository memcachedRepository
                                    , PlatformTransactionManager transactionManager
                                    , DataService<MetricLevelLtd> dataService
                                    , InstrumentAttributeService instrumentAttributeService
    , SettingsService settingsService
    , AttributeService attributeService
    , AccountingPeriodService accountingPeriodService
    , ExecutionStateService executionStateService
    , TransactionActivityService activityService) {
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
        this.memcachedRepository = memcachedRepository;
        this.transactionManager = transactionManager;
        this.dataService = dataService;
        this.settingsService = settingsService;
        this.instrumentAttributeService = instrumentAttributeService;
        this.attributeService = attributeService;
        this.accountingPeriodService = accountingPeriodService;
        this.executionStateService = executionStateService;
        this.activityService = activityService;
    }

    @Bean("transactionActivityUploadJob")
    public Job transactionActivityUploadJob(TransactionActivityJobCompletionListener listener, Step transactionActivityImportStep) {
        return new JobBuilder("transactionActivityUploadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(transactionActivityImportStep)
                .end()
                .build();
    }

    @Bean
    public Step transactionActivityImportStep() {
        return new StepBuilder("transactionActivityImportStep", jobRepository)
                .<TransactionActivity, TransactionActivity>chunk(10, new ResourcelessTransactionManager())
                .reader(transactionActivityFileReader(""))
                .processor(transactionActivityItemProcessor())
                .writer(transactionActivityItemWriter(dataSourceProvider
                        , tenantContextHolder
                        , this.memcachedRepository
                        , this.instrumentAttributeService
                        , this.attributeService
                        , this.accountingPeriodService))
                .build();
    }

    @Bean
    public ItemProcessor<TransactionActivity, TransactionActivity> transactionActivityItemProcessor() {
        return new TransactionActivityItemProcessor();
    }

    @Bean()
    @StepScope
    public FlatFileItemReader<TransactionActivity> transactionActivityFileReader(@Value("#{jobParameters[filePath]}") String fileName) {

        DefaultLineMapper<TransactionActivity> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

        lineTokenizer.setNames(new String[] {"ACTIVITYUPLOADID" ,"POSTINGDATE" ,"TRANSACTIONDATE","INSTRUMENTID","TRANSACTIONNAME","AMOUNT","ATTRIBUTEID"});
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(new FieldSetMapper<TransactionActivity>() {
            @Override
            public TransactionActivity mapFieldSet(FieldSet fieldSet) throws BindException {
                TransactionActivity transactionActivity = new TransactionActivity();
                Date postingDate = DateUtil.stripTime(fieldSet.readDate("POSTINGDATE"));
                int postingDateIntValue = DateUtil.dateInNumber(postingDate);
                transactionActivity.setPostingDate(postingDateIntValue);
                transactionActivity.setTransactionDate(DateUtil.stripTime(fieldSet.readDate("TRANSACTIONDATE")));
                transactionActivity.setInstrumentId(fieldSet.readString("INSTRUMENTID"));
                transactionActivity.setTransactionName(fieldSet.readString("TRANSACTIONNAME"));
                transactionActivity.setAmount(fieldSet.readDouble("AMOUNT"));
                transactionActivity.setAttributeId(fieldSet.readString("ATTRIBUTEID"));

                return transactionActivity;
            }
        });

        return new FlatFileItemReaderBuilder<TransactionActivity>()
                .name("transactionActivityItemReader")
                .resource(new FileSystemResource(fileName))
                .delimited()
                .names(new String[]{
                        "ACTIVITYUPLOADID","POSTINGDATE","TRANSACTIONDATE","INSTRUMENTID","TRANSACTIONNAME","AMOUNT","ATTRIBUTEID"
                })
                .linesToSkip(1)
                .lineMapper(defaultLineMapper)
                .build();
    }

    private void validateFile(String filename) {
        if (!filename.isEmpty()) {
            Resource resource = new FileSystemResource("file:" + filename);
            if (!resource.exists() || !resource.isReadable()) {
                throw new IllegalArgumentException("File " + filename + " does not exist or is not readable");
            }
        }
    }

    @Bean
    public ItemWriter<TransactionActivity> transactionActivityItemWriter(TenantDataSourceProvider dataSourceProvider,
                                                          TenantContextHolder tenantContextHolder
            , MemcachedRepository memcachedRepository
            , InstrumentAttributeService instrumentAttributeService
            , AttributeService attributeService
            , AccountingPeriodService accountingPeriodService) {
        MongoItemWriter<TransactionActivity> delegate = new MongoItemWriterBuilder<TransactionActivity>()
                .template(mongoTemplate)
                .collection("TransactionActivity")
                .build();
        return new TransactionActivityItemWriter(delegate
                , dataSourceProvider
                , tenantContextHolder
                , memcachedRepository
                , instrumentAttributeService
                , attributeService
                , accountingPeriodService);
    }

    @Bean
    @StepScope
    public JobParameters jobParametersAccessor(@Value("#{jobParameters}") JobParameters jobParameters) {
        return jobParameters;
    }

    @Bean("attributeAggregationJob")
    public Job attributeAggregationJob() {
        return new JobBuilder("attributeAggregationJob", this.jobRepository)
                .start(attributeLevelAggregationStep())
                .build();
    }

    @Bean("instrumentAggregationJob")
    public Job instrumentAggregationJob() {
        return new JobBuilder("instrumentAggregationJob", this.jobRepository)
                .start(instrumentLevelAggregationStep())
                .build();
    }

    @Bean("metricAggregationJob")
    public Job metricAggregationJob() {
        return new JobBuilder("metricAggregationJob", this.jobRepository)
                .start(metricLevelAggregationStep())
                .build();
    }


    @Bean("updateInstrumentActivitySateJob")
    public Job updateInstrumentActivitySateJob() {
        return new JobBuilder("updateInstrumentActivitySateJob", this.jobRepository)
                .start(instrumentActivityStateStep())
                .build();
    }

    @Bean
    public Step metricLevelAggregationStep() {
        return new StepBuilder("metricLevelAggregationStep", jobRepository)
                .tasklet(metricLevelAggregatorTasklet(),transactionManager)
                .build();
    }

    @Bean
    public Step instrumentLevelAggregationStep() {
        return new StepBuilder("instrumentLevelAggregationStep", jobRepository)
                .tasklet(instrumentLevelAggregatorTasklet(), transactionManager)
                .build();
    }

    @Bean
    public Step attributeLevelAggregationStep() {
        return new StepBuilder("attributeLevelAggregationStep", jobRepository)
                .tasklet(attributeLevelAggregatorTasklet(), transactionManager)
                .build();
    }

    @Bean
    public Step instrumentActivityStateStep() {
        return  new StepBuilder("instrumentActivityStateStep", jobRepository)
                .tasklet(instrumentActivityStateTasklet(), transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet metricLevelAggregatorTasklet() {
        return new MetricLevelAggregatorTasklet(this.memcachedRepository, this.dataService, this.settingsService, this.executionStateService,this.dataService.getTenantId());
    }

    @Bean
    @StepScope
    public Tasklet instrumentLevelAggregatorTasklet() {
        return new InstrumentLevelAggregatorTasklet(this.memcachedRepository, this.dataService, this.settingsService, this.executionStateService, this.dataService.getTenantId());
    }

    @Bean
    @StepScope
    public Tasklet attributeLevelAggregatorTasklet() {
        return new AttributeLevelAggregatorTasklet(this.memcachedRepository, this.dataService, this.settingsService, this.executionStateService,this.dataService.getTenantId());
    }

    @Bean
    @StepScope
    public Tasklet instrumentActivityStateTasklet() {
        return new InstrumentActivityStateTasklet(activityService);
    }
}

