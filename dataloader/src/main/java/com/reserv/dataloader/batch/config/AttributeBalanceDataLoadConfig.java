package com.reserv.dataloader.batch.config;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.AttributeLevelLtd;
import com.reserv.dataloader.batch.processor.AttributeBalanceItemProcessor;
import com.reserv.dataloader.batch.writer.GenericItemWriterAdapter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.data.builder.MongoItemWriterBuilder;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.IOException;
import java.util.Map;

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class AttributeBalanceDataLoadConfig {

    private final JobRepository jobRepository;
    private final TenantDataSourceProvider dataSourceProvider;
    private MongoTemplate mongoTemplate;
    private final TenantContextHolder tenantContextHolder;
    private final BatchCommonConfig batchCommonConfig;

    public AttributeBalanceDataLoadConfig(JobRepository jobRepository, MongoTemplate mongoTemplate,
                                             TenantDataSourceProvider dataSourceProvider,
                                          TenantContextHolder tenantContextHolder,
                                          BatchCommonConfig batchCommonConfig) {
        this.jobRepository = jobRepository;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
        this.tenantContextHolder = tenantContextHolder;
        this.batchCommonConfig = batchCommonConfig;
    }

    @Bean("attributeBalanceUploadJob")
    public Job attributeBalanceUploadJob(Step attributeBalanceImportStep) {
        return new JobBuilder("attributeBalanceUploadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .flow(attributeBalanceImportStep)
                .end()
                .build();
    }

    @Bean
    public Step attributeBalanceImportStep() throws IOException {
        return new StepBuilder("attributeBalanceImportStep", jobRepository)
                .<Map<String,Object>, AttributeLevelLtd>chunk(10, new ResourcelessTransactionManager())
                .reader(this.batchCommonConfig.genericReader(""))
                .processor(attributeBalanceMapItemProcessor())
                .writer(attributeBalanceWriter(dataSourceProvider
                        , this.tenantContextHolder
                       ))
                .build();
    }


    @Bean
    public ItemProcessor<Map<String,Object>, AttributeLevelLtd> attributeBalanceMapItemProcessor() {
        return new AttributeBalanceItemProcessor();
    }



    @Bean
    public ItemWriter<AttributeLevelLtd> attributeBalanceWriter(TenantDataSourceProvider dataSourceProvider,
                                                                             TenantContextHolder tenantContextHolder) {
        MongoItemWriter<AttributeLevelLtd> delegate = new MongoItemWriterBuilder<AttributeLevelLtd>()
                .template(mongoTemplate)
                .collection("AttributeLevelLtd")
                .build();
        return new GenericItemWriterAdapter<>(delegate, dataSourceProvider, tenantContextHolder);
    }
}
