package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.AggregateItemProcessor;
import com.reserv.dataloader.batch.writer.AggregationItemWriter;
import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.Aggregation;
import com.reserv.dataloader.repository.AggregationMemcachedRepository;
import com.reserv.dataloader.batch.listener.AggregationValidationLoggingListener;
import com.reserv.dataloader.validation.AggregationValidator;
import com.fyntrac.common.service.TransactionService;
import com.reserv.dataloader.batch.exception.ItemValidationException;
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
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.validation.BindException;

import java.util.List;

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class AggregationDataLoadConfig {

    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoTemplate mongoTemplate;
    private final AggregationMemcachedRepository memcachedRepository;
    public AggregationDataLoadConfig(JobRepository jobRepository, MongoTemplate mongoTemplate,
                                    TenantDataSourceProvider dataSourceProvider,
                                    TenantContextHolder tenantContextHolder,
                                     AggregationMemcachedRepository memcachedRepository) {
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
        this.memcachedRepository = memcachedRepository;
    }

    @Bean("aggregationUploadJob")
    public Job aggregationUploadJob(JobCompletionNotificationListener listener, Step aggregationImportStep) {
        return new JobBuilder("aggregationUploadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(aggregationImportStep)
                .end()
                .build();
    }

    @Bean
    public Step aggregationImportStep(
            @org.springframework.beans.factory.annotation.Autowired(required = false) com.fyntrac.common.repository.RefDataValidationLogRepository validationLogRepository,
            AggregationValidationLoggingListener validationLoggingListener,
            AggregationValidator validator,
            TransactionService transactionService,
            com.fyntrac.common.service.aggregation.AggregationService aggregationService) {
        ItemProcessor<Aggregation, Aggregation> processor = aggregateItemProcessor(
                validator, 
                transactionService, 
                aggregationService,
                validationLogRepository);
        return new StepBuilder("aggregationImportStep", jobRepository)
                .<Aggregation, Aggregation>chunk(10, new ResourcelessTransactionManager())
                .reader(aggregateFileReader(""))
                .processor(processor)
                .faultTolerant()
                .skip(ItemValidationException.class)
                .skipLimit(Integer.MAX_VALUE)
                .listener(validationLoggingListener)
                .listener(processor)
                .writer(aggregationItemWriter(dataSourceProvider,
                        tenantContextHolder, this.memcachedRepository))
                .build();
    }

    @Bean
    @StepScope
    public AggregateItemProcessor aggregateItemProcessor(
            AggregationValidator validator,
            TransactionService transactionService,
            com.fyntrac.common.service.aggregation.AggregationService aggregationService,
            com.fyntrac.common.repository.RefDataValidationLogRepository validationLogRepository) {
        return new AggregateItemProcessor(validator, transactionService, aggregationService, validationLogRepository);
    }

    @Bean()
    @StepScope
    public FlatFileItemReader<Aggregation> aggregateFileReader(@Value("#{jobParameters[filePath]}") String fileName) {

        List<String> headerNames;
        try {
            headerNames = getHeaderNames(fileName);
        } catch (Exception e) {
            log.error("Failed to read header names from file: " + fileName, e);
            headerNames = java.util.Arrays.asList("ACTIVITYUPLOADID", "TRANSACTIONNAME", "METRICNAME");
        }

        DefaultLineMapper<Aggregation> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setQuoteCharacter('"');
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames(headerNames.toArray(new String[0]));
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(new FieldSetMapper<Aggregation>() {
            @Override
            public Aggregation mapFieldSet(FieldSet fieldSet) throws BindException {
                Aggregation aggregation = new Aggregation();
                // Read raw values safely so Validator can verify exactly what was inputted (case, spaces, emptiness)
                aggregation.setTransactionName(readStringSafe(fieldSet, "TRANSACTIONNAME", "TRANSACTION NAME", "TRANSACTION_NAME"));
                aggregation.setMetricName(readStringSafe(fieldSet, "METRICNAME", "METRIC NAME", "METRIC_NAME"));

                return aggregation;
            }

            private String readStringSafe(FieldSet fieldSet, String... names) {
                for (String name : names) {
                    try {
                        String val = fieldSet.readString(name);
                        if (val != null) {
                            return val;
                        }
                    } catch (IllegalArgumentException e) {
                        // ignore and try next
                    }
                }
                return "";
            }
        });

        return new FlatFileItemReaderBuilder<Aggregation>()
                .name("aggregateDataItemReader")
                .resource(new FileSystemResource(fileName))
                .delimited()
                .names(headerNames.toArray(new String[0]))
                .linesToSkip(1)
                .lineMapper(defaultLineMapper)
                .build();
    }

    private java.util.List<String> getHeaderNames(String filePath) throws java.io.IOException {
        try (java.io.Reader reader = java.nio.file.Files.newBufferedReader(java.nio.file.Paths.get(filePath));
             org.apache.commons.csv.CSVParser parser = new org.apache.commons.csv.CSVParser(reader, org.apache.commons.csv.CSVFormat.DEFAULT.withQuote('"'))) {
            org.apache.commons.csv.CSVRecord headerRecord = parser.iterator().next();
            java.util.List<String> headers = new java.util.ArrayList<>();
            for (String header : headerRecord) {
                headers.add(header.trim().toUpperCase());
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
    public ItemWriter<Aggregation> aggregationItemWriter(TenantDataSourceProvider dataSourceProvider,
                                                       TenantContextHolder tenantContextHolder,
                                                         AggregationMemcachedRepository memcachedRepository) {
        MongoItemWriter<Aggregation> delegate = new MongoItemWriterBuilder<Aggregation>()
                .template(mongoTemplate)
                .collection("Aggregation")
                .build();
        return new AggregationItemWriter(delegate, dataSourceProvider, tenantContextHolder, memcachedRepository);
    }

}

