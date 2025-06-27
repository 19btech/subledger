package com.reserv.dataloader.batch.config;

import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.component.TransactionActivityQueue;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.AttributeLevelLtd;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.fyntrac.common.service.aggregation.AttributeLevelAggregationService;
import com.fyntrac.common.utils.Key;
import com.reserv.dataloader.batch.processor.AttributeLevelLtdProcessor;
import com.reserv.dataloader.batch.reader.TransactionActivityItemReader;
import com.reserv.dataloader.batch.writer.AttributeLevelLtdFlatteningWriter;
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
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;
import java.util.Set;

@Configuration
@EnableBatchProcessing(modular = true)
public class AttributeLevelLtdBatchConfig {

    private final MemcachedRepository memcachedRepository;
    private final TransactionActivityQueue transactionActivityQueue;
    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private final AggregationService aggregationService;
    private final AttributeLevelAggregationService attributeLevelAggregationService;

    @Value("${fyntrac.chunk.size}")
    private int chunkSize;

    public AttributeLevelLtdBatchConfig(JobRepository jobRepository,
                                        TenantContextHolder tenantContextHolder,
                                        TenantDataSourceProvider dataSourceProvider,
                                        MemcachedRepository memcachedRepository,
                                        TransactionActivityQueue transactionActivityQueue,
                                        AggregationService aggregationService,
                                        AttributeLevelAggregationService attributeLevelAggregationService) {
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.memcachedRepository = memcachedRepository;
        this.transactionActivityQueue = transactionActivityQueue;
        this.aggregationService = aggregationService;
        this.attributeLevelAggregationService = attributeLevelAggregationService;
    }


    // === Step ===
    @Bean
    public Step attributeLevelLtdStep() throws Exception {
        return new StepBuilder("attribute-level-ltd-step", jobRepository)
                .<TransactionActivity, List<Records.AttributeLevelLtdRecord>>chunk(chunkSize, new ResourcelessTransactionManager())
                .reader(attributeLevelLtdItemReader("", 0L, this.transactionActivityQueue))
                .processor(attributeLevelLtdProcessor(""))
                .writer(attributeLevelLtdWriter("", 0L, this.aggregationService.getDataService().getMongoTemplate()))
                .build();
    }
    // === Job ===
    @Bean
    public Job attributeLevelLtdJob(Step attributeLevelLtdStep) {
        return new JobBuilder("attributeLevelLtdJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(attributeLevelLtdStep)
                .build();
    }

    // === Reader ===
    @Bean
    @StepScope
    public TransactionActivityItemReader attributeLevelLtdItemReader(
            @Value("#{jobParameters['tenantId']}") String tenantId,
            @Value("#{jobParameters['jobId']}") Long jobId,
            TransactionActivityQueue activityQueue
    ) {
        return new TransactionActivityItemReader(activityQueue.getIterator(tenantId, jobId));
    }


    // === Processor ===
    @Bean
    @StepScope
    public ItemProcessor<TransactionActivity,  List<Records.AttributeLevelLtdRecord>> attributeLevelLtdProcessor(
            @Value("#{jobParameters['tenantId']}") String tenantId
    ) {
        aggregationService.setTenant(tenantId);
        aggregationService.loadIntoCache();

        String metricKey = Key.allMetricList(tenantId);
        CacheMap<Set<String>> metrics = memcachedRepository.getFromCache(metricKey, CacheMap.class);

        return new AttributeLevelLtdProcessor(
                metrics.getMap()
        );
    }

    // === Writer ===
    @Bean
    @StepScope
    public ItemWriter<List<Records.AttributeLevelLtdRecord>> attributeLevelLtdWriter(
            @Value("#{jobParameters['tenantId']}") String tenantId,
            @Value("#{jobParameters['jobId']}") Long jobId,
            MongoTemplate mongoTemplate
    ) throws Exception {
        MongoItemWriter<AttributeLevelLtd> delegate = new MongoItemWriterBuilder<AttributeLevelLtd>()
                .template(mongoTemplate)
                .collection("AttributeLevelLtd")
                .build();
        delegate.afterPropertiesSet();
        return new AttributeLevelLtdFlatteningWriter(delegate, dataSourceProvider, tenantContextHolder, memcachedRepository, this.attributeLevelAggregationService, mongoTemplate, tenantId, jobId);
    }
}
