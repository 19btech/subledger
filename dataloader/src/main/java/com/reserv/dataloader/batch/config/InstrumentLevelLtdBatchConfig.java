package com.reserv.dataloader.batch.config;


import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.component.TransactionActivityQueue;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.InstrumentLevelLtd;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.fyntrac.common.service.aggregation.InstrumentLevelAggregationService;
import com.fyntrac.common.utils.Key;
import com.reserv.dataloader.batch.processor.InstrumentLevelLtdProcessor;
import com.reserv.dataloader.batch.reader.TransactionActivityItemReader;
import com.reserv.dataloader.batch.writer.InstrumentLevelLtdFlatteningWriter;
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
public class InstrumentLevelLtdBatchConfig {

    private final MongoTemplate mongoTemplate;
    private final MemcachedRepository memcachedRepository;
    private final TransactionActivityQueue transactionActivityQueue;
    private final JobRepository jobRepository;
    private final TenantContextHolder tenantContextHolder;
    private final TenantDataSourceProvider dataSourceProvider;
    private final AggregationService aggregationService;
    private final InstrumentLevelAggregationService instrumentLevelAggregationService;

    @Value("${fyntrac.chunk.size}")
    private int chunkSize;
    public InstrumentLevelLtdBatchConfig(JobRepository jobRepository,
                                        TenantContextHolder tenantContextHolder,
                                        TenantDataSourceProvider dataSourceProvider,
                                        MemcachedRepository memcachedRepository,
                                        TransactionActivityQueue transactionActivityQueue,
                                        AggregationService aggregationService,
                                         InstrumentLevelAggregationService instrumentLevelAggregationService) {
        this.memcachedRepository = memcachedRepository;
        this.transactionActivityQueue = transactionActivityQueue;
        this.jobRepository = jobRepository;
        this.tenantContextHolder = tenantContextHolder;
        this.dataSourceProvider = dataSourceProvider;
        this.aggregationService = aggregationService;
        this.aggregationService.setTenant(TenantContextHolder.getTenant());
        this.mongoTemplate = this.aggregationService.getDataService().getMongoTemplate();
        this.instrumentLevelAggregationService=instrumentLevelAggregationService;

    }

    // === Step ===
    @Bean
    public Step instrumentLevelLtdStep() throws Exception {
        return new StepBuilder("instrument-level-ltd-step", jobRepository)
                .<TransactionActivity, List<Records.InstrumentLevelLtdRecord>>chunk(chunkSize, new ResourcelessTransactionManager())
                .reader(instrumentLevelLtdItemReader("", 0L, this.transactionActivityQueue))
                .processor(instrummentLevelLtdProcessor("", 0L))
                .writer(instrumentLevelLtdItemWriter("", 0L, this.aggregationService.getDataService().getMongoTemplate()))
                .build();
    }

    // === Job ===
    @Bean
    public Job instrumentLevelLtdJob() throws Throwable{

        return new JobBuilder("instrumentLevelLtdJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                //.listener(listener)
                .flow(instrumentLevelLtdStep())
                .end()
                .build();
    }

    // === Reader ===
    @Bean
    @StepScope
    public TransactionActivityItemReader instrumentLevelLtdItemReader(@Value("#{jobParameters['tenantId']}") String tenantId,
                                                                       @Value("#{jobParameters['jobId']}") Long jobId
            , TransactionActivityQueue activityQueue
    ) {
        return new TransactionActivityItemReader(activityQueue.getIterator(tenantId, jobId));
    }

    // === Processor ===
    @Bean
    @StepScope
    public ItemProcessor<TransactionActivity,  List<Records.InstrumentLevelLtdRecord>> instrummentLevelLtdProcessor(
            @Value("#{jobParameters['tenantId']}") String tenantId,
            @Value("#{jobParameters['jobId']}") Long jobId
    ) {
        aggregationService.setTenant(tenantId);
        aggregationService.loadIntoCache();

        String metricKey = Key.allMetricList(tenantId);
        CacheMap<Set<String>> metrics = memcachedRepository.getFromCache(metricKey, CacheMap.class);

        return new InstrumentLevelLtdProcessor(
                tenantId,
                jobId,
                metrics.getMap()
        );
    }

    // === Writer ===
    @Bean
    @StepScope
    public ItemWriter<List<Records.InstrumentLevelLtdRecord>> instrumentLevelLtdItemWriter(
            @Value("#{jobParameters['tenantId']}") String tenantId,
            @Value("#{jobParameters['jobId']}") Long jobId,
            MongoTemplate mongoTemplate
    ) throws Exception {
        MongoItemWriter<InstrumentLevelLtd> delegate = new MongoItemWriterBuilder<InstrumentLevelLtd>()
                .template(mongoTemplate)
                .collection("InstrumentLevelLtd")
                .build();
        delegate.afterPropertiesSet();
        return new InstrumentLevelLtdFlatteningWriter(delegate, dataSourceProvider, tenantContextHolder, memcachedRepository, mongoTemplate, this.instrumentLevelAggregationService, tenantId, jobId);
    }
}


