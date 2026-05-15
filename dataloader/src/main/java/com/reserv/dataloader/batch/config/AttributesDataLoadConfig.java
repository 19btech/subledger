package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.AttributesItemProcessor;
import com.reserv.dataloader.batch.writer.AttributeItemWriter;
import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.component.TenantDataSourceProvider;
import  com.fyntrac.common.enums.DataType;
import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.utils.StringUtil;
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

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class AttributesDataLoadConfig {

    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private MongoTemplate mongoTemplate;

    public AttributesDataLoadConfig(JobRepository jobRepository, MongoTemplate mongoTemplate,
                                      TenantDataSourceProvider dataSourceProvider,
                                      TenantContextHolder tenantContextHolder) {
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
    }

    @Bean("attributeUploadJob")
    public Job attributeUploadJob(JobCompletionNotificationListener listener, Step attributeImportStep) {
        return new JobBuilder("attributeUploadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(attributeImportStep)
                .end()
                .build();
    }

    @Bean
    public Step attributeImportStep(
            ItemProcessor<Attributes, Attributes> attributeItemProcessor,
            ItemReader<Attributes> attributeFileReader,
            ItemWriter<Attributes> attributesItemWriter,
            com.reserv.dataloader.batch.listener.ValidationLoggingListener validationLoggingListener) {
        
        return new StepBuilder("attributeImportStep", jobRepository)
                .<Attributes, Attributes>chunk(10, new ResourcelessTransactionManager())
                .reader(attributeFileReader)
                .processor(attributeItemProcessor)
                .faultTolerant()
                .skip(com.reserv.dataloader.batch.exception.ItemValidationException.class)
                .skipLimit(Integer.MAX_VALUE)
                .listener(validationLoggingListener)
                .listener(attributeItemProcessor)
                .writer(attributesItemWriter)
                .build();
    }

    @Bean
    @StepScope
    public ItemProcessor<Attributes, Attributes> attributeItemProcessor(
            com.reserv.dataloader.validation.AttributesValidator validator,
            @org.springframework.beans.factory.annotation.Autowired(required = false) com.fyntrac.common.repository.AttributesRepository attributesRepository,
            @org.springframework.beans.factory.annotation.Autowired(required = false) com.fyntrac.common.repository.RefDataValidationLogRepository validationLogRepository) {
        return new AttributesItemProcessor(validator, attributesRepository, validationLogRepository);
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Attributes> attributeFileReader(@Value("#{jobParameters[filePath]}") String fileName) {
        log.info("Initializing attributeFileReader for file: {}", fileName);
        DefaultLineMapper<Attributes> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[] {"ACTIVITYUPLOADID", "USERFIELD", "ATTRIBUTENAME", "RECLASSABLE", "VERSIONABLE", "DATATYPE", "NULLABLE"});
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(new FieldSetMapper<Attributes>() {
            @Override
            public Attributes mapFieldSet(FieldSet fieldSet) throws BindException {
                Attributes attribute = new Attributes();
                attribute.setAttributeName(readStringSafe(fieldSet, "ATTRIBUTENAME"));
                attribute.setUserField(readStringSafe(fieldSet, "USERFIELD"));
                
                // Pass raw string data for Enum/Nullable validation directly to transient fields
                attribute.setRawDataType(readStringSafe(fieldSet, "DATATYPE"));
                attribute.setRawNullable(readStringSafe(fieldSet, "NULLABLE"));

                // Map validation sentinel integer flags (-1 = invalid, -2 = empty)
                attribute.setIsReclassable(parseBooleanFlag(readStringSafe(fieldSet, "RECLASSABLE")));
                attribute.setIsVersionable(parseBooleanFlag(readStringSafe(fieldSet, "VERSIONABLE")));
                
                // Note: The AttributesValidator will assign default values on warning and resolve final mappings.
                return attribute;
            }

            private String readStringSafe(FieldSet fieldSet, String name) {
                try {
                    return fieldSet.readString(name);
                } catch (IllegalArgumentException e) {
                    return "";
                }
            }

            private int parseBooleanFlag(String val) {
                if (val == null || val.trim().isEmpty())
                    return -2; // missing
                val = val.trim().toLowerCase();
                if (val.equals("true") || val.equals("1") || val.equals("1.0") || val.equals("yes") || val.equals("y"))
                    return 1;
                if (val.equals("false") || val.equals("0") || val.equals("0.0") || val.equals("no") || val.equals("n"))
                    return 0;
                return -1; // invalid
            }
        });

        return new FlatFileItemReaderBuilder<Attributes>()
                .name("attributeDataItemReader")
                .resource(new FileSystemResource(fileName))
                .delimited()
                .names(new String[]{
                        "ACTIVITYUPLOADID", "USERFIELD", "ATTRIBUTENAME", "RECLASSABLE", "VERSIONABLE", "DATATYPE", "NULLABLE"
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
    public ItemWriter<Attributes> attributesItemWriter(TenantDataSourceProvider dataSourceProvider,
                                                      TenantContextHolder tenantContextHolder) {
        MongoItemWriter<Attributes> delegate = new MongoItemWriterBuilder<Attributes>()
                .template(mongoTemplate)
                .collection("Attributes")
                .build();

        return new AttributeItemWriter(delegate, dataSourceProvider, tenantContextHolder);
    }

}
