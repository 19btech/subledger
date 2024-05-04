package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.TransactionActivityItemProcessor;
import com.reserv.dataloader.batch.writer.GenericItemWriterAdapter;
import com.reserv.dataloader.component.TenantContextHolder;
import com.reserv.dataloader.component.TenantDataSourceProvider;
import com.reserv.dataloader.entity.TransactionActivity;
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
public class TransactionActivityDataLoadConfig {

    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoTemplate mongoTemplate;

    public TransactionActivityDataLoadConfig(JobRepository jobRepository, MongoTemplate mongoTemplate,
                                     TenantDataSourceProvider dataSourceProvider,
                                     TenantContextHolder tenantContextHolder) {
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
    }

    @Bean("transactionActivityUploadJob")
    public Job transactionActivityUploadJob(JobCompletionNotificationListener listener, Step transactionActivityImportStep) {
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
                .writer(transactionActivityItemWriter(dataSourceProvider,
                        tenantContextHolder))
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

        lineTokenizer.setNames(new String[] {"ACTIVITYUPLOADID","TRANSACTIONDATE","INSTRUMENTID","TRANSACTIONNAME","AMOUNT","ATTRIBUTEID"});
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(new FieldSetMapper<TransactionActivity>() {
            @Override
            public TransactionActivity mapFieldSet(FieldSet fieldSet) throws BindException {
                TransactionActivity transactionActivity = new TransactionActivity();
                transactionActivity.setTransactionDate(fieldSet.readDate("TRANSACTIONDATE"));
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
                        "ACTIVITYUPLOADID","TRANSACTIONDATE","INSTRUMENTID","TRANSACTIONNAME","AMOUNT","ATTRIBUTEID"
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
                                                          TenantContextHolder tenantContextHolder) {
        MongoItemWriter<TransactionActivity> delegate = new MongoItemWriterBuilder<TransactionActivity>()
                .template(mongoTemplate)
                .collection("TransactionActivity")
                .build();
        return new GenericItemWriterAdapter<>(delegate, dataSourceProvider, tenantContextHolder);
    }

}

