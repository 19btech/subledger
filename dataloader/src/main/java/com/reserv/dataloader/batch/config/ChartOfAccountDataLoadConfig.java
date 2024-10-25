package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.mapper.HeaderColumnNameMapper;
import com.reserv.dataloader.batch.processor.ChartOfAccountItemProcessor;
import com.reserv.dataloader.batch.writer.GenericItemWriterAdapter;
import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.ChartOfAccount;
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
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class ChartOfAccountDataLoadConfig {
    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private MongoTemplate mongoTemplate;

    public ChartOfAccountDataLoadConfig(JobRepository jobRepository, MongoTemplate mongoTemplate,
                                             TenantDataSourceProvider dataSourceProvider,
                                             TenantContextHolder tenantContextHolder) {
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
    }

    @Bean("chartOfAccountUploadJob")
    public Job chartOfAccountUploadJob(JobCompletionNotificationListener listener, Step chartOfAccountImportStep) {
        return new JobBuilder("chartOfAccountUploadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(chartOfAccountImportStep)
                .end()
                .build();
    }

    @Bean
    public Step chartOfAccountImportStep() {
        return new StepBuilder("chartOfAccountImportStep", jobRepository)
                .<Map<String,Object>, ChartOfAccount>chunk(10, new ResourcelessTransactionManager())
                .reader(chartOfAccountReader("", ""))
                .processor(chartOfAccountMapItemProcessor())
                .writer(chartOfAccountWriter(dataSourceProvider,
                        tenantContextHolder))
                .build();
    }

    @Bean
    public ItemProcessor<Map<String,Object>,ChartOfAccount> chartOfAccountMapItemProcessor() {
        return new ChartOfAccountItemProcessor();
    }

    @StepScope
    @Bean
    public FlatFileItemReader<Map<String, Object>> chartOfAccountReader(@Value("#{jobParameters[filePath]}") String fileName
            ,@Value("#{jobParameters[columnName]}") String columnNames) {


        FlatFileItemReader<Map<String, Object>> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(fileName));
        DefaultLineMapper<Map<String, Object>> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        String[] columns = columnNames.split(",");
        List<String> colList = new ArrayList<>(0);
        Map<String, String> colMap = new HashMap<>(0);

        for(String col : columns) {
            String[] c = col.split(":");
            colList.add(c[0]);
            colMap.put(c[0],c[1]);
        }

        tokenizer.setNames(colList.toArray(new String[0]));
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new HeaderColumnNameMapper());
        reader.setLinesToSkip(1); // Skip the first line
        reader.setLineMapper(lineMapper);
        return reader;

    }


    @Bean
    public ItemWriter<ChartOfAccount> chartOfAccountWriter(TenantDataSourceProvider dataSourceProvider,
                                                          TenantContextHolder tenantContextHolder) {
        MongoItemWriter<ChartOfAccount> delegate = new MongoItemWriterBuilder<ChartOfAccount>()
                .template(mongoTemplate)
                .collection("ChartOfAccount")
                .build();
        return new GenericItemWriterAdapter<>(delegate, dataSourceProvider, tenantContextHolder);
    }
}
