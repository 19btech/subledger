package com.reserv.dataloader.batch.config;


import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.SubledgerMappingItemProcessor;
import com.reserv.dataloader.batch.writer.SubledgerMappingWriter;
import com.reserv.dataloader.config.TenantContextHolder;
import com.reserv.dataloader.component.TenantDataSourceProvider;
import com.fyntrac.common.enums.EntryType;
import com.fyntrac.common.enums.Sign;
import com.fyntrac.common.entity.SubledgerMapping;
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

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class SubledgerMappingDataLoadConfig {

    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private MongoTemplate mongoTemplate;

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
    public Step subledgerMappingImportStep() {
        return new StepBuilder("subledgerMappingImportStep", jobRepository)
                .<SubledgerMapping, SubledgerMapping>chunk(10, new ResourcelessTransactionManager())
                .reader(subledgerMappingFileReader(""))
                .processor(subledgerMappingItemProcessor())
                .writer(subledgerMappingItemWriter(dataSourceProvider,
                        tenantContextHolder))
                .build();
    }

    @Bean
    public ItemProcessor<SubledgerMapping, SubledgerMapping> subledgerMappingItemProcessor() {
        return new SubledgerMappingItemProcessor();
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
                String sign = fieldSet.readString("SIGN");
                if(Sign.isValid(sign)) {
                    subledgerMapping.setSign(Sign.valueOf(sign));
                }else {
                    subledgerMapping.setSign(Sign.POSITIVE);
                }

                String entryType = fieldSet.readString("ENTRYTYPE");

                if(EntryType.isValid(entryType)) {
                    subledgerMapping.setEntryType(EntryType.valueOf(entryType));
                }else{
                    subledgerMapping.setEntryType(EntryType.CREDIT);
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
    public ItemWriter<SubledgerMapping> subledgerMappingItemWriter(TenantDataSourceProvider dataSourceProvider,
                                                               TenantContextHolder tenantContextHolder) {
        MongoItemWriter<SubledgerMapping> delegate = new MongoItemWriterBuilder<SubledgerMapping>()
                .template(mongoTemplate)
                .collection("SubledgerMapping")
                .build();
        return new SubledgerMappingWriter(delegate, dataSourceProvider, tenantContextHolder);
    }
}

