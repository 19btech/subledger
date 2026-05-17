package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.SubledgerMappingItemProcessor;
import com.reserv.dataloader.batch.writer.SubledgerMappingWriter;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.SubledgerMapping;
import com.reserv.dataloader.validation.SubledgerMappingValidator;
import com.fyntrac.common.repository.TransactionsRepository;
import com.fyntrac.common.repository.AccountTypesRepository;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.batch.listener.ValidationLoggingListener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.ItemProcessListener;
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

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class SubledgerMappingDataLoadConfig {

    public static final ThreadLocal<RawValidationContext> RAW_CONTEXT = ThreadLocal.withInitial(RawValidationContext::new);

    public static class RawValidationContext {
        public String rawSign;
        public String rawEntryType;
    }

    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoTemplate mongoTemplate;

    public SubledgerMappingDataLoadConfig(JobRepository jobRepository, MongoTemplate mongoTemplate,
                                        TenantDataSourceProvider dataSourceProvider,
                                        TenantContextHolder tenantContextHolder) {
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
    }

    @Bean("subledgerMappingUploadJob")
    public Job subledgerMappingUploadJob(JobCompletionNotificationListener listener, Step subledgerMappingImportStep) {
        return new JobBuilder("subledgerMappingUploadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(subledgerMappingImportStep)
                .end()
                .build();
    }

    @Bean
    public Step subledgerMappingImportStep(
            SubledgerMappingItemProcessor subledgerMappingItemProcessor,
            ItemReader<SubledgerMapping> subledgerMappingFileReader,
            ItemWriter<SubledgerMapping> subledgerMappingItemWriter,
            ValidationLoggingListener validationLoggingListener) {
        org.springframework.batch.item.ItemWriter<SubledgerMapping> writer = subledgerMappingItemWriter;
        org.springframework.batch.core.step.builder.SimpleStepBuilder<SubledgerMapping, SubledgerMapping> builder =
                new StepBuilder("subledgerMappingImportStep", jobRepository)
                .<SubledgerMapping, SubledgerMapping>chunk(10, new ResourcelessTransactionManager())
                .reader(subledgerMappingFileReader)
                .processor(subledgerMappingItemProcessor)
                .writer(writer);

        builder.faultTolerant()
                .skip(ItemValidationException.class)
                .skipLimit(Integer.MAX_VALUE);

        builder.listener((org.springframework.batch.core.StepExecutionListener) validationLoggingListener);
        builder.listener((org.springframework.batch.core.ItemProcessListener<?, ?>) validationLoggingListener);
        builder.listener((org.springframework.batch.core.StepExecutionListener) subledgerMappingItemProcessor);

        return builder.build();
    }

    @Bean
    @StepScope
    public SubledgerMappingItemProcessor subledgerMappingItemProcessor(
            SubledgerMappingValidator validator,
            TransactionsRepository transactionsRepository,
            AccountTypesRepository accountTypesRepository,
            RefDataValidationLogRepository validationLogRepository) {
        return new SubledgerMappingItemProcessor(validator, transactionsRepository, accountTypesRepository, validationLogRepository);
    }

    @Bean()
    @StepScope
    public FlatFileItemReader<SubledgerMapping> subledgerMappingFileReader(@Value("#{jobParameters[filePath]}") String fileName) {

        DefaultLineMapper<SubledgerMapping> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[] {"ACTIVITYUPLOADID", "TRANSACTIONNAME", "SIGN", "ENTRYTYPE", "ACCOUNTSUBTYPE"});
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(new FieldSetMapper<SubledgerMapping>() {
            @Override
            public SubledgerMapping mapFieldSet(FieldSet fieldSet) throws BindException {
                SubledgerMapping subledgerMapping = new SubledgerMapping();
                
                String rawSign = fieldSet.readString("SIGN");
                String rawEntryType = fieldSet.readString("ENTRYTYPE");
                
                // Store raw values in thread-local context for robust validation in the validator phase
                RawValidationContext ctx = RAW_CONTEXT.get();
                ctx.rawSign = rawSign;
                ctx.rawEntryType = rawEntryType;
                
                // Safe-map Sign enum
                if (com.fyntrac.common.enums.Sign.isValid(rawSign)) {
                    String cleanSign = rawSign.trim().toUpperCase();
                    subledgerMapping.setSign(com.fyntrac.common.enums.Sign.valueOf(cleanSign));
                } else {
                    subledgerMapping.setSign(null);
                }
                
                // Safe-map EntryType enum
                if (com.fyntrac.common.enums.EntryType.isValid(rawEntryType)) {
                    String cleanEntryType = rawEntryType.trim();
                    if (cleanEntryType.equalsIgnoreCase("DEBIT")) {
                        subledgerMapping.setEntryType(com.fyntrac.common.enums.EntryType.DEBIT);
                    } else if (cleanEntryType.equalsIgnoreCase("CREDIT")) {
                        subledgerMapping.setEntryType(com.fyntrac.common.enums.EntryType.CREDIT);
                    }
                } else {
                    subledgerMapping.setEntryType(null);
                }
                
                subledgerMapping.setTransactionName(fieldSet.readString("TRANSACTIONNAME"));
                subledgerMapping.setAccountSubType(fieldSet.readString("ACCOUNTSUBTYPE"));
                return subledgerMapping;
            }
        });

        return new FlatFileItemReaderBuilder<SubledgerMapping>()
                .name("activityUploadDataItemReader")
                .resource(new FileSystemResource(fileName))
                .delimited()
                .names(new String[]{
                        "ACTIVITYUPLOADID", "TRANSACTIONNAME", "SIGN", "ENTRYTYPE", "ACCOUNTSUBTYPE"
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
    @StepScope
    public ItemWriter<SubledgerMapping> subledgerMappingItemWriter(TenantDataSourceProvider dataSourceProvider,
                                                               TenantContextHolder tenantContextHolder) {
        MongoItemWriter<SubledgerMapping> delegate = new MongoItemWriterBuilder<SubledgerMapping>()
                .template(mongoTemplate)
                .collection("SubledgerMapping")
                .build();
        return new SubledgerMappingWriter(delegate, dataSourceProvider, tenantContextHolder);
    }
}
