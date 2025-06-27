package com.reserv.dataloader.batch.config;

import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.component.TransactionActivityQueue;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.MetricLevelLtd;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.fyntrac.common.service.aggregation.MetricLevelAggregationService;
import com.fyntrac.common.utils.Key;
import com.reserv.dataloader.batch.processor.MetricLevelLtdProcessor;
import com.reserv.dataloader.batch.reader.TransactionActivityItemReader;
import com.reserv.dataloader.batch.writer.MetricLevelLtdFlatteningWriter;
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
public class MetricLevelLtdBatchConfig {

    private final MongoTemplate mongoTemplate;
    private final MemcachedRepository memcachedRepository;
    private final TransactionActivityQueue transactionActivityQueue;
    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private final AggregationService aggregationService;
    private final MetricLevelAggregationService metricLevelAggregationService;

    @Value("${fyntrac.chunk.size}")
    private int chunkSize;
    public MetricLevelLtdBatchConfig(JobRepository jobRepository,
                                         TenantContextHolder tenantContextHolder,
                                         TenantDataSourceProvider dataSourceProvider,
                                         MemcachedRepository memcachedRepository,
                                         TransactionActivityQueue transactionActivityQueue,
                                         AggregationService aggregationService,
                                     MetricLevelAggregationService metricLevelAggregationService) {
        this.memcachedRepository = memcachedRepository;
        this.transactionActivityQueue = transactionActivityQueue;
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.aggregationService = aggregationService;
        this.aggregationService.setTenant(TenantContextHolder.getTenant());
        this.mongoTemplate = this.aggregationService.getDataService().getMongoTemplate();
        this.metricLevelAggregationService = metricLevelAggregationService;

    }

    // === Step ===
    @Bean
    public Step metricLevelLtdStep() throws Exception {
        return new StepBuilder("metric-level-ltd-step", jobRepository)
                .<TransactionActivity, List<Records.MetricLevelLtdRecord>>chunk(chunkSize, new ResourcelessTransactionManager())
                .reader(metricItemReader("", 0L, this.transactionActivityQueue))
                .processor(metricLevelLtdProcessor("", 0L))
                .writer(metricLevelLtdItemWriter("", 0L, this.aggregationService.getDataService().getMongoTemplate()))
                .build();
    }

    // === Job ===
    @Bean
    public Job metricLevelLtdJob() throws Throwable{

        return new JobBuilder("metricLevelLtdJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                //.listener(listener)
                .flow(metricLevelLtdStep())
                .end()
                .build();
    }

    // === Reader ===
    @Bean
    @StepScope
    public TransactionActivityItemReader metricItemReader(@Value("#{jobParameters['tenantId']}") String tenantId,
                                                                       @Value("#{jobParameters['jobId']}") Long jobId
            , TransactionActivityQueue activityQueue
    ) {
        return new TransactionActivityItemReader(activityQueue.getIterator(tenantId, jobId));
    }

    // === Processor ===
    @Bean
    @StepScope
    public ItemProcessor<TransactionActivity,  List<Records.MetricLevelLtdRecord>> metricLevelLtdProcessor(
            @Value("#{jobParameters['tenantId']}") String tenantId,
            @Value("#{jobParameters['jobId']}") Long jobId
    ) {
        aggregationService.setTenant(tenantId);
        aggregationService.loadIntoCache();

        String metricKey = Key.allMetricList(tenantId);
        CacheMap<Set<String>> metrics = memcachedRepository.getFromCache(metricKey, CacheMap.class);

        return new MetricLevelLtdProcessor(
                tenantId,
                jobId,
                metrics.getMap()
        );
    }

    // === Writer ===
    @Bean
    @StepScope
    public ItemWriter<List<Records.MetricLevelLtdRecord>> metricLevelLtdItemWriter(
            @Value("#{jobParameters['tenantId']}") String tenantId,
            @Value("#{jobParameters['jobId']}") Long jobId,
            MongoTemplate mongoTemplate
    ) throws Exception {
        MongoItemWriter<MetricLevelLtd> delegate = new MongoItemWriterBuilder<MetricLevelLtd>()
                .template(mongoTemplate)
                .collection("MetricLevelLtd")
                .build();
        delegate.afterPropertiesSet();
        return new MetricLevelLtdFlatteningWriter(delegate, dataSourceProvider, tenantContextHolder, memcachedRepository, mongoTemplate, this.metricLevelAggregationService, tenantId, jobId);
    }
}
