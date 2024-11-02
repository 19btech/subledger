package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.listener.InstrumentAttributeJobCompletionListener;
import com.reserv.dataloader.batch.mapper.HeaderColumnNameMapper;
import com.reserv.dataloader.batch.processor.InstrumentAttributeItemProcessor;
import com.reserv.dataloader.batch.writer.InstrumentAttributeWriter;
import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.service.InstrumentAttributeService;
import com.fyntrac.common.entity.InstrumentAttribute;
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
import com.fyntrac.common.repository.MemcachedRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class InstrumentAttributeDataLoadConfig {
    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private MongoTemplate mongoTemplate;
    private MemcachedRepository memcachedRepository;
    private InstrumentAttributeService instrumentAttributeService;


    public InstrumentAttributeDataLoadConfig(JobRepository jobRepository, MongoTemplate mongoTemplate,
                                             TenantDataSourceProvider dataSourceProvider,
                                             TenantContextHolder tenantContextHolder,
                                             MemcachedRepository memcachedRepository,
                                             InstrumentAttributeService instrumentAttributeService) {
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
        this.memcachedRepository = memcachedRepository;
        this.instrumentAttributeService = instrumentAttributeService;
    }

    @Bean("instrumentAttributeUploadJob")
    public Job instrumentAttributeUploadJob(InstrumentAttributeJobCompletionListener listener, Step instrumentAttributeImportStep) {
        return new JobBuilder("instrumentAttributeUploadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(instrumentAttributeImportStep)
                .end()
                .build();
    }

    @Bean
    public Step instrumentAttributeImportStep() {
        return new StepBuilder("instrumentAttributeImportStep", jobRepository)
                .<Map<String,Object>,InstrumentAttribute>chunk(10, new ResourcelessTransactionManager())
                .reader(instrumentAttributeReader("", ""))
                .processor(instrumentAttributeMapItemProcessor())
                .writer(instrumentAttributeWriter(dataSourceProvider,
                        tenantContextHolder, this.memcachedRepository, this.instrumentAttributeService))
                .build();
    }

    @Bean
    public ItemProcessor<Map<String,Object>,InstrumentAttribute> instrumentAttributeMapItemProcessor() {
        return new InstrumentAttributeItemProcessor();
    }

    @StepScope
    @Bean
    public FlatFileItemReader<Map<String, Object>> instrumentAttributeReader(@Value("#{jobParameters[filePath]}") String fileName
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
//        lineMapper.setFieldSetMapper(fields -> {
//            Map<String, Object> map = Map.of(
//                    "effectiveDate", fields.readString("effectiveDate"),
//                    "instrumentId", fields.readString("instrumentId"),
//                    "attributeId", fields.readString("attributeId"),
//                    "key1", fields.readString("key1"),
//                    "key2", fields.readString("key2"),
//                    "key3", fields.readString("key3")
//            );
//            return map;
//        });
        reader.setLinesToSkip(1); // Skip the first line
        reader.setLineMapper(lineMapper);
        return reader;

    }


    @Bean
    public ItemWriter<InstrumentAttribute> instrumentAttributeWriter(TenantDataSourceProvider dataSourceProvider,
                                                                     TenantContextHolder tenantContextHolder
                                                            , MemcachedRepository memcachedRepository
                                                            , InstrumentAttributeService instrumentAttributeService) {
        MongoItemWriter<InstrumentAttribute> delegate = new MongoItemWriterBuilder<InstrumentAttribute>()
                .template(mongoTemplate)
                .collection("InstrumentAttribute")
                .build();

        return new InstrumentAttributeWriter(delegate, dataSourceProvider, tenantContextHolder, memcachedRepository, instrumentAttributeService);
    }
}
