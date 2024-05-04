package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.AggregateItemProcessor;
import com.reserv.dataloader.batch.writer.GenericItemWriterAdapter;
import com.reserv.dataloader.component.TenantContextHolder;
import com.reserv.dataloader.component.TenantDataSourceProvider;
import com.reserv.dataloader.datasource.accounting.rule.AggregationLevel;
import com.reserv.dataloader.entity.Aggregation;
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
public class AggregationDataLoadConfig {

    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoTemplate mongoTemplate;

    public AggregationDataLoadConfig(JobRepository jobRepository, MongoTemplate mongoTemplate,
                                    TenantDataSourceProvider dataSourceProvider,
                                    TenantContextHolder tenantContextHolder) {
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
    }

    @Bean("aggregationUploadJob")
    public Job aggregationUploadJob(JobCompletionNotificationListener listener, Step aggregationImportStep) {
        return new JobBuilder("aggregationUploadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(aggregationImportStep)
                .end()
                .build();
    }

    @Bean
    public Step aggregationImportStep() {
        return new StepBuilder("aggregationImportStep", jobRepository)
                .<Aggregation, Aggregation>chunk(10, new ResourcelessTransactionManager())
                .reader(aggregateFileReader(""))
                .processor(aggregateItemProcessor())
                .writer(aggregationItemWriter(dataSourceProvider,
                        tenantContextHolder))
                .build();
    }

    @Bean
    public ItemProcessor<Aggregation, Aggregation> aggregateItemProcessor() {
        return new AggregateItemProcessor();
    }

    @Bean()
    @StepScope
    public FlatFileItemReader<Aggregation> aggregateFileReader(@Value("#{jobParameters[filePath]}") String fileName) {

        DefaultLineMapper<Aggregation> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[] {"ACTIVITYUPLOADID", "TRANSACTIONNAME",	"METRICNAME", "LEVEL"});
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(new FieldSetMapper<Aggregation>() {
            @Override
            public Aggregation mapFieldSet(FieldSet fieldSet) throws BindException {
                Aggregation aggregation = new Aggregation();
                aggregation.setTransactionName(fieldSet.readString("TRANSACTIONNAME"));
                aggregation.setMetricName(fieldSet.readString("METRICNAME"));

                String level = fieldSet.readString("LEVEL");
                if(AggregationLevel.isValid(level)) {
                    aggregation.setLevel(AggregationLevel.valueOf(level));
                }else {
                    aggregation.setLevel(AggregationLevel.ATTRIBUTE);
                }

                return aggregation;
            }
        });

        return new FlatFileItemReaderBuilder<Aggregation>()
                .name("aggregateDataItemReader")
                .resource(new FileSystemResource(fileName))
                .delimited()
                .names(new String[]{
                        "ACTIVITYUPLOADID", "TRANSACTIONNAME",	"METRICNAME", "LEVEL"
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
    public ItemWriter<Aggregation> aggregationItemWriter(TenantDataSourceProvider dataSourceProvider,
                                                       TenantContextHolder tenantContextHolder) {
        MongoItemWriter<Aggregation> delegate = new MongoItemWriterBuilder<Aggregation>()
                .template(mongoTemplate)
                .collection("Aggregation")
                .build();
        return new GenericItemWriterAdapter<>(delegate, dataSourceProvider, tenantContextHolder);
    }

}

