package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.AccountTypeItemProcessor;
import com.reserv.dataloader.batch.writer.GenericItemWriterAdapter;
import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.component.TenantDataSourceProvider;
import  com.fyntrac.common.enums.AccountType;
import com.fyntrac.common.entity.AccountTypes;
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
    public Step accountTypeImportStep(
            ItemProcessor<AccountTypes, AccountTypes> accountTypeItemProcessor,
            ItemReader<AccountTypes> accountTypeFileReader,
            ItemWriter<AccountTypes> accountTypeItemWriter,
            com.reserv.dataloader.batch.listener.ValidationLoggingListener validationLoggingListener) {
        return new StepBuilder("accountTypeImportStep", jobRepository)
                .<AccountTypes, AccountTypes>chunk(10, new ResourcelessTransactionManager())
                .reader(accountTypeFileReader)
                .processor(accountTypeItemProcessor)
                .faultTolerant()
                .skip(com.reserv.dataloader.batch.exception.ItemValidationException.class)
                .skipLimit(Integer.MAX_VALUE)
                .listener((org.springframework.batch.core.StepExecutionListener) validationLoggingListener)
                .listener((org.springframework.batch.core.ItemProcessListener) validationLoggingListener)
                // Registering the processor itself as a listener too: .processor(...) alone does not
                // wire up its StepExecutionListener callbacks — without this, beforeStep() never ran
                // regardless of the @Bean return-type fix above, and the DB preload never happened.
                .listener(accountTypeItemProcessor)
                .writer(accountTypeItemWriter)
                .build();
    }

    // Declared to return the concrete AccountTypeItemProcessor type, not the ItemProcessor
    // interface: with @StepScope's TARGET_CLASS proxy mode, Spring needs the factory method's
    // return type to be a concrete class to CGLIB-subclass it. Returning the bare interface here
    // made Spring silently fall back to a JDK interface-only proxy that exposes nothing but
    // process(Object) — beforeStep() never existed on that proxy, so it never fired, the DB
    // preload of existing account subtypes never ran, and duplicate account types slipped
    // through undetected (same bug as TransactionsDataLoadConfig/AttributesDataLoadConfig, and
    // this is the one that's actually wired up — see AccountTypeUploadService).
    @Bean
    @StepScope
    public AccountTypeItemProcessor accountTypeItemProcessor(
            com.reserv.dataloader.validation.AccountTypesValidator validator,
            @org.springframework.beans.factory.annotation.Autowired(required = false) com.fyntrac.common.repository.AccountTypesRepository accountTypesRepository,
            @org.springframework.beans.factory.annotation.Autowired(required = false) com.fyntrac.common.repository.RefDataValidationLogRepository validationLogRepository) {
        return new AccountTypeItemProcessor(validator, accountTypesRepository, validationLogRepository);
    }

    @Bean()
    @StepScope
    public FlatFileItemReader<AccountTypes> accountTypeFileReader(@Value("#{jobParameters[filePath]}") String fileName) {

        DefaultLineMapper<AccountTypes> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[] {"ACTIVITYUPLOADID","ACCOUNTSUBTYPE","ACCOUNTTYPE"});
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(new FieldSetMapper<AccountTypes>() {
            @Override
            public AccountTypes mapFieldSet(FieldSet fieldSet) throws BindException {
                AccountTypes accountType = new AccountTypes();
                accountType.setAccountSubType(fieldSet.readString("ACCOUNTSUBTYPE"));
                String accType = fieldSet.readString("ACCOUNTTYPE");
                accType = accType.replaceAll("\\s+", "");

                if(AccountType.isValid(accType)) {
                    accountType.setAccountType(AccountType.valueOf(accType.toUpperCase()));
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
                        "ACTIVITYUPLOADID","ACCOUNTSUBTYPE","ACCOUNTTYPE"
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


