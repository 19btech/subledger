package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.TransactionsItemProcessor;
import com.reserv.dataloader.batch.writer.TransactionItemWriter;
import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.component.TenantDataSourceProvider;
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
    public Step transactionImportStep() {
        return new StepBuilder("transactionImportStep", jobRepository)
                .<Transactions, Transactions>chunk(10, new ResourcelessTransactionManager())
                .reader(transactionFileReader(""))
                .processor(transactionsItemProcessor())
                .writer(transactionWriter(dataSourceProvider,
                        tenantContextHolder))
                .build();
    }

    @Bean
    public ItemProcessor<Transactions, Transactions> transactionsItemProcessor() {
        return new TransactionsItemProcessor();
    }

    @Bean()
    @StepScope
    public FlatFileItemReader<Transactions> transactionFileReader(@Value("#{jobParameters[filePath]}") String fileName) {

        DefaultLineMapper<Transactions> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[] {"ACTIVITYUPLOADID", "NAME","ISREPLAYABLE","EXCLUSIVE","ISGL"});
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(new FieldSetMapper<Transactions>() {
            @Override
            public Transactions mapFieldSet(FieldSet fieldSet) throws BindException {
                Transactions transaction = new Transactions();
                transaction.setName(fieldSet.readString("NAME"));
                transaction.setExclusive(fieldSet.readInt("EXCLUSIVE"));
                transaction.setIsGL(fieldSet.readInt("ISGL"));
                transaction.setIsReplayable(fieldSet.readInt("ISREPLAYABLE"));
                return transaction;
            }
        });

        return new FlatFileItemReaderBuilder<Transactions>()
                .name("activityUploadDataItemReader")
                .resource(new FileSystemResource(fileName))
                .delimited()
                .names(new String[]{
                        "ACTIVITYUPLOADID", "NAME","ISREPLAYABLE","EXCLUSIVE","ISGL"
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
    public ItemWriter<Transactions> transactionWriter(TenantDataSourceProvider dataSourceProvider,
                                                TenantContextHolder tenantContextHolder) {
        MongoItemWriter<Transactions> delegate = new MongoItemWriterBuilder<Transactions>()
                .template(mongoTemplate)
                .collection("Transactions")
                .build();

        return new TransactionItemWriter(delegate, dataSourceProvider, tenantContextHolder);
    }

}