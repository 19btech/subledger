package com.reserv.dataloader.batch.config;

import com.fyntrac.common.component.InstrumentReplayQueue;
import com.fyntrac.common.component.InstrumentReplaySet;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.component.TransactionActivityQueue;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.MetricLevelLtd;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.*;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.fyntrac.common.service.aggregation.AttributeLevelAggregationService;
import com.fyntrac.common.service.aggregation.InstrumentLevelAggregationService;
import com.fyntrac.common.service.aggregation.MetricLevelAggregationService;
import com.reserv.dataloader.batch.listener.TransactionActivityJobCompletionListener;
import com.reserv.dataloader.batch.mapper.HeaderColumnNameMapper;
import com.reserv.dataloader.batch.processor.TransactionActivityItemProcessor;
import com.reserv.dataloader.batch.tasklet.AttributeLevelAggregatorTasklet;
import com.reserv.dataloader.batch.tasklet.InstrumentLevelAggregatorTasklet;
import com.reserv.dataloader.batch.tasklet.MetricLevelAggregatorTasklet;
import com.reserv.dataloader.batch.writer.TransactionActivityItemWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    private final AggregationService aggregationService;
    private  final AttributeLevelAggregationService attributeLevelAggregationService;
    private  final InstrumentLevelAggregationService instrumentLevelAggregationService;
    private final MetricLevelAggregationService metricLevelAggregationService;
    private final TransactionService transactionService;
    private final TransactionActivityQueue transactionActivityQueue;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("M/d/yyyy");
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
    , AggregationService aggregationService
    , AttributeLevelAggregationService attributeLevelAggregationService
    , InstrumentLevelAggregationService instrumentLevelAggregationService
    , MetricLevelAggregationService metricLevelAggregationService
    , TransactionService transactionService
    , TransactionActivityQueue transactionActivityQueue
    ) {
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
        this.aggregationService = aggregationService;
        this.attributeLevelAggregationService = attributeLevelAggregationService;
        this.instrumentLevelAggregationService = instrumentLevelAggregationService;
        this.metricLevelAggregationService = metricLevelAggregationService;
        this.transactionService = transactionService;
        this.transactionActivityQueue = transactionActivityQueue;

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
    public Step transactionActivityImportStep() throws IOException {
        return new StepBuilder("transactionActivityImportStep", jobRepository)
                .<Map<String, Object>, TransactionActivity>chunk(10, new ResourcelessTransactionManager())
                .reader(transactionActivityReader(""))
                .processor(transactionActivityItemProcessor())
                .writer(transactionActivityItemWriter(dataSourceProvider
                        , tenantContextHolder
                        , this.memcachedRepository
                        , this.instrumentAttributeService
                        , this.attributeService
                        , this.accountingPeriodService
                , this.transactionActivityQueue
                ))
                .build();
    }

    @Bean
    public ItemProcessor<Map<String,Object>, TransactionActivity> transactionActivityItemProcessor() {
        return new TransactionActivityItemProcessor();
    }

    @StepScope
    @Bean
    public FlatFileItemReader<Map<String, Object>> transactionActivityReader(
            @Value("#{jobParameters['filePath']}") String filePath) throws IOException {

        FlatFileItemReader<Map<String, Object>> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(filePath));
        reader.setLinesToSkip(1); // Skip the header

        // Read actual header line using Apache Commons CSV to get clean column names
        List<String> headerNames = getHeaderNames(filePath);

        // Setup line tokenizer
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setQuoteCharacter('"'); // Handles quoted fields correctly
        tokenizer.setStrict(false); // Allow rows with missing fields
        tokenizer.setNames(headerNames.toArray(new String[0]));

        // Line mapper
        DefaultLineMapper<Map<String, Object>> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new HeaderColumnNameMapper());

        reader.setLineMapper(lineMapper);
        return reader;

    }

    private List<String> getHeaderNames(String filePath) throws IOException {
        try (Reader reader = Files.newBufferedReader(Paths.get(filePath));
             CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withQuote('"'))) {

            CSVRecord headerRecord = parser.iterator().next();
            List<String> headers = new ArrayList<>();
            for (String header : headerRecord) {
                headers.add(header.trim());
            }
            return headers;
        }
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
            , AccountingPeriodService accountingPeriodService
    , TransactionActivityQueue transactionActivityQueue
    ) {
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
                , accountingPeriodService
        , transactionService
        , executionStateService
        , transactionActivityQueue
        );
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

    @Bean("postUploadActivityJob")
    public Job postUploadActivityJob() {
        return new JobBuilder("postUploadActivityJob", this.jobRepository)
                .start(postUploadActivityStep())
                .build();
    }

    @Bean
    public Step postUploadActivityStep() {
        return new StepBuilder("postUploadActivityStep", jobRepository)
                .tasklet(postUploadActivityStepTasklet(),transactionManager)
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
    @StepScope
    public Tasklet postUploadActivityStepTasklet() {
        return new MetricLevelAggregatorTasklet(this.memcachedRepository, this.dataService, this.settingsService, this.executionStateService,accountingPeriodService, this.aggregationService, this.metricLevelAggregationService, this.transactionActivityQueue,this.dataService.getTenantId());
    }

    @Bean
    @StepScope
    public Tasklet metricLevelAggregatorTasklet() {
        return new MetricLevelAggregatorTasklet(this.memcachedRepository, this.dataService, this.settingsService, this.executionStateService,accountingPeriodService, this.aggregationService, this.metricLevelAggregationService, this.transactionActivityQueue,this.dataService.getTenantId());
    }

    @Bean
    @StepScope
    public Tasklet instrumentLevelAggregatorTasklet() {
        return new InstrumentLevelAggregatorTasklet(this.memcachedRepository, this.dataService, this.settingsService, this.executionStateService, this.accountingPeriodService, this.aggregationService,this.instrumentLevelAggregationService, this.transactionActivityQueue,this.dataService.getTenantId());
    }

    @Bean
    @StepScope
    public Tasklet attributeLevelAggregatorTasklet() {
        return new AttributeLevelAggregatorTasklet(this.memcachedRepository, this.dataService, this.settingsService, this.executionStateService, this.accountingPeriodService, this.aggregationService,this.attributeLevelAggregationService, this.transactionActivityQueue,this.dataService.getTenantId());
    }


}

