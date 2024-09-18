package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.AccountTypeItemProcessor;
import com.reserv.dataloader.batch.writer.GenericItemWriterAdapter;
import com.reserv.dataloader.config.TenantContextHolder;
import com.reserv.dataloader.component.TenantDataSourceProvider;
import com.reserv.dataloader.datasource.accounting.rule.AccountType;
import com.reserv.dataloader.entity.AccountTypes;
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
public class AccountTypeDataLoadConfig {

    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoTemplate mongoTemplate;

    public AccountTypeDataLoadConfig(JobRepository jobRepository, MongoTemplate mongoTemplate,
                                     TenantDataSourceProvider dataSourceProvider,
                                     TenantContextHolder tenantContextHolder) {
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
    }

    @Bean("accountTypeUploadJob")
    public Job accountTypeUploadJob(JobCompletionNotificationListener listener, Step accountTypeImportStep) {
        return new JobBuilder("accountTypeUploadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(accountTypeImportStep)
                .end()
                .build();
    }

    @Bean
    public Step accountTypeImportStep() {
        return new StepBuilder("accountTypeImportStep", jobRepository)
                .<AccountTypes, AccountTypes>chunk(10, new ResourcelessTransactionManager())
                .reader(accountTypeFileReader(""))
                .processor(accountTypeItemProcessor())
                .writer(accountTypeItemWriter(dataSourceProvider,
                        tenantContextHolder))
                .build();
    }

    @Bean
    public ItemProcessor<AccountTypes, AccountTypes> accountTypeItemProcessor() {
        return new AccountTypeItemProcessor();
    }

    @Bean()
    @StepScope
    public FlatFileItemReader<AccountTypes> accountTypeFileReader(@Value("#{jobParameters[filePath]}") String fileName) {

        DefaultLineMapper<AccountTypes> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[] {"ACTIVITYUPLOADID","ACCOUNTTYPE","ACCOUNTSUBTYPE"});
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(new FieldSetMapper<AccountTypes>() {
            @Override
            public AccountTypes mapFieldSet(FieldSet fieldSet) throws BindException {
                AccountTypes accountType = new AccountTypes();
                accountType.setAccountSubType(fieldSet.readString("ACCOUNTSUBTYPE"));
                String accType = fieldSet.readString("ACCOUNTTYPE");

                if(AccountType.isValid(accType)) {
                    accountType.setAccountType(AccountType.valueOf(accType));
                }else {
                    accountType.setAccountType(AccountType.INCOMESTATEMENT);
                }

                return accountType;
            }
        });

        return new FlatFileItemReaderBuilder<AccountTypes>()
                .name("accountTypeDataItemReader")
                .resource(new FileSystemResource(fileName))
                .delimited()
                .names(new String[]{
                        "ACTIVITYUPLOADID","ACCOUNTTYPE","ACCOUNTSUBTYPE"
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
    public ItemWriter<AccountTypes> accountTypeItemWriter(TenantDataSourceProvider dataSourceProvider,
                                                         TenantContextHolder tenantContextHolder) {
        MongoItemWriter<AccountTypes> delegate = new MongoItemWriterBuilder<AccountTypes>()
                .template(mongoTemplate)
                .collection("AccountTypes")
                .build();
        return new GenericItemWriterAdapter<>(delegate, dataSourceProvider, tenantContextHolder);
    }

}


