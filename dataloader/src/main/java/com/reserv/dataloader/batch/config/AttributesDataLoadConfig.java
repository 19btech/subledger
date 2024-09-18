package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.AttributesItemProcessor;
import com.reserv.dataloader.batch.writer.AttributeItemWriter;
import com.reserv.dataloader.config.TenantContextHolder;
import com.reserv.dataloader.component.TenantDataSourceProvider;
import com.reserv.dataloader.datasource.accounting.rule.DataType;
import com.reserv.dataloader.entity.Attributes;
import com.reserv.dataloader.utils.StringUtil;
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
    public Step attributeImportStep() {
        return new StepBuilder("attributeImportStep", jobRepository)
                .<Attributes, Attributes>chunk(10, new ResourcelessTransactionManager())
                .reader(attributeFileReader(""))
                .processor(attributeItemProcessor())
                .writer(attributesItemWriter(dataSourceProvider,
                        tenantContextHolder))
                .build();
    }

    @Bean
    public ItemProcessor<Attributes, Attributes> attributeItemProcessor() {
        return new AttributesItemProcessor();
    }

    @Bean()
    @StepScope
    public FlatFileItemReader<Attributes> attributeFileReader(@Value("#{jobParameters[filePath]}") String fileName) {

        DefaultLineMapper<Attributes> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[] {"ACTIVITYUPLOADID", "USERFIELD", "ATTRIBUTENAME", "ISRECLASSABLE", "DATATYPE", "ISNULLABLE"});
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(new FieldSetMapper<Attributes>() {
            @Override
            public Attributes mapFieldSet(FieldSet fieldSet) throws BindException {
                Attributes attribute = new Attributes();
                attribute.setAttributeName(fieldSet.readString("ATTRIBUTENAME"));
                attribute.setUserField(fieldSet.readString("USERFIELD"));
                String dataType = fieldSet.readString("DATATYPE");
                if(DataType.isValid(dataType)) {
                    attribute.setDataType(DataType.valueOf(dataType));
                }else {
                    attribute.setDataType(DataType.STRING);
                }

                attribute.setIsReclassable(StringUtil.parseBoolean(fieldSet.readString("ISRECLASSABLE")));
                attribute.setIsNullable(StringUtil.parseBoolean(fieldSet.readString("ISNULLABLE")));
                return attribute;
            }
        });

        return new FlatFileItemReaderBuilder<Attributes>()
                .name("attributeDataItemReader")
                .resource(new FileSystemResource(fileName))
                .delimited()
                .names(new String[]{
                        "ACTIVITYUPLOADID", "USERFIELD", "ATTRIBUTENAME", "ISRECLASSABLE", "DATATYPE", "ISNULLABLE"
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
    public ItemWriter<Attributes> attributesItemWriter(TenantDataSourceProvider dataSourceProvider,
                                                      TenantContextHolder tenantContextHolder) {
        MongoItemWriter<Attributes> delegate = new MongoItemWriterBuilder<Attributes>()
                .template(mongoTemplate)
                .collection("Attributes")
                .build();

        return new AttributeItemWriter(delegate, dataSourceProvider, tenantContextHolder);
    }

}
