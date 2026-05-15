package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.TransactionsItemProcessor;
import com.reserv.dataloader.batch.writer.TransactionItemWriter;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.Transactions;
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
import org.springframework.batch.item.ItemReader;
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
public class TransactionsDataLoadConfig {

    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private MongoTemplate mongoTemplate;

    public TransactionsDataLoadConfig(JobRepository jobRepository, MongoTemplate mongoTemplate,
            TenantDataSourceProvider dataSourceProvider,
            TenantContextHolder tenantContextHolder) {
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
    }

    @Bean("transactionsUploadJob")
    public Job transactionsUploadJob(JobCompletionNotificationListener listener, Step transactionImportStep) {
        return new JobBuilder("transactionsUploadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(transactionImportStep)
                .end()
                .build();
    }

    @Bean
    public Step transactionImportStep(
            ItemProcessor<Transactions, Transactions> transactionsItemProcessor,
            ItemReader<Transactions> transactionFileReader,
            ItemWriter<Transactions> transactionWriter,
            com.reserv.dataloader.batch.listener.ValidationLoggingListener validationLoggingListener) {
        return new StepBuilder("transactionImportStep", jobRepository)
                .<Transactions, Transactions>chunk(10, new ResourcelessTransactionManager())
                .reader(transactionFileReader)
                .processor(transactionsItemProcessor)
                .faultTolerant()
                .skip(com.reserv.dataloader.batch.exception.ItemValidationException.class)
                .skipLimit(Integer.MAX_VALUE)
                .listener(validationLoggingListener)
                .listener(transactionsItemProcessor)
                .writer(transactionWriter)
                .build();
    }

    @Bean
    @StepScope
    public ItemProcessor<Transactions, Transactions> transactionsItemProcessor(
            com.reserv.dataloader.validation.TransactionValidator validator,
            com.fyntrac.common.repository.RefDataValidationLogRepository validationLogRepository,
            com.fyntrac.common.repository.MemcachedRepository memcachedRepository) {
        return new TransactionsItemProcessor(validator, validationLogRepository, memcachedRepository);
    }

    @Bean()
    @StepScope
    public FlatFileItemReader<Transactions> transactionFileReader(
            @Value("#{jobParameters[filePath]}") String fileName) {

        List<String> headerNames;
        try {
            headerNames = getHeaderNames(fileName);
        } catch (Exception e) {
            log.error("Failed to read header names from file: " + fileName, e);
            headerNames = java.util.Arrays.asList("ACTIVITYUPLOADID", "NAME", "ISREPLAYABLE", "EXCLUSIVE", "ISGL");
        }

        DefaultLineMapper<Transactions> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setQuoteCharacter('"');
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames(headerNames.toArray(new String[0]));
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(new FieldSetMapper<Transactions>() {
            @Override
            public Transactions mapFieldSet(FieldSet fieldSet) throws BindException {
                Transactions transaction = new Transactions();
                transaction.setName(readStringSafe(fieldSet, "NAME"));
                transaction.setExclusive(parseExclusive(readStringSafe(fieldSet, "EXCLUSIVE", "REPORTABLE")));
                transaction.setIsGL(parseBooleanFlag(readStringSafe(fieldSet, "ISGL", "JOURNAL")));
                transaction.setIsReplayable(parseBooleanFlag(readStringSafe(fieldSet, "ISREPLAYABLE", "REPLAYABLE")));
                return transaction;
            }

            private String readStringSafe(FieldSet fieldSet, String... names) {
                for (String name : names) {
                    try {
                        return fieldSet.readString(name);
                    } catch (IllegalArgumentException e) {
                        // ignore and try next
                    }
                }
                return "";
            }

            private int parseBooleanFlag(String val) {
                if (val == null || val.trim().isEmpty())
                    return -2; // missing
                val = val.trim().toLowerCase();
                if (val.equals("true") || val.equals("1") || val.equals("1.0"))
                    return 1;
                if (val.equals("false") || val.equals("0") || val.equals("0.0"))
                    return 0;
                return -1; // invalid
            }

            private int parseExclusive(String val) {
                if (val == null || val.trim().isEmpty())
                    return 0;
                try {
                    return (int) Double.parseDouble(val.trim());
                } catch (NumberFormatException e) {
                    return -1;
                }
            }
        });

        return new FlatFileItemReaderBuilder<Transactions>()
                .name("activityUploadDataItemReader")
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
    @StepScope
    public ItemWriter<Transactions> transactionWriter(TenantDataSourceProvider dataSourceProvider,
            TenantContextHolder tenantContextHolder) {
        MongoItemWriter<Transactions> delegate = new MongoItemWriterBuilder<Transactions>()
                .template(mongoTemplate)
                .collection("Transactions")
                .build();

        return new TransactionItemWriter(delegate, dataSourceProvider, tenantContextHolder);
    }

}