package com.reserv.dataloader.batch.config;

import com.fyntrac.common.entity.MetricLevelLtd;
import com.fyntrac.common.service.DataService;
import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.PostAggregationMetricLevelLtdProcessor;
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
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class MetricLevelPostAggregationConfig {

    @Autowired
    private JobRepository jobRepository;
    private final DataService<MetricLevelLtd> dataService;
    @Value("${fyntrac.chunk.size}")
    private int chunkSize;

    @Autowired
    public MetricLevelPostAggregationConfig(DataService<MetricLevelLtd> dataService) {
        this.dataService = dataService;
    }

    @Bean("metricLevelPostAggregationJob")
    public Job metricLevelPostAggregationJob(JobCompletionNotificationListener listener, Step postAggregationMetricLevelStep) {
        return new JobBuilder("metricLevelPostAggregationJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(postAggregationMetricLevelStep)
                .end()
                .build();
    }

    @Bean
    public Step postAggregationMetricLevelStep() {
        return new StepBuilder("postAggregationMetricLevelStep", jobRepository)
                .<MetricLevelLtd, MetricLevelLtd>chunk(chunkSize, new ResourcelessTransactionManager())
                .reader(metricLevelPostAggregationReader(0, this.dataService))
                .processor(postAggregationMetricLevelLtdItemProcessor(0))
                .writer(postAggregationMetricLevelLtdItemWriter())
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<? extends MetricLevelLtd> metricLevelPostAggregationReader(
            @Value("#{jobParameters['fromDate']}") long fromDate,
            DataService<MetricLevelLtd> dataService) {

        return new ItemReader<>() {
            private Iterator<MetricLevelLtd> resultIterator;

            @Override
            public MetricLevelLtd read() {
                if (resultIterator == null) {

                    MatchOperation matchStage = Aggregation.match(
                            Criteria.where("postingDate").gte(fromDate));

                    GroupOperation groupStage = Aggregation.group("metricName", "metricId")
                            .count().as("count")
                            .first("metricName").as("metricName")
                            .first("metricId").as("metricId")
                            .first("postingDate").as("postingDate")
                            .first("accountingPeriodId").as("accountingPeriodId")
                            .first("balance").as("balance");

                    MatchOperation havingCountOneStage = Aggregation.match(
                            Criteria.where("count").is(1));

                    Aggregation aggregation = Aggregation.newAggregation(
                            matchStage, groupStage, havingCountOneStage);

                    AggregationResults<MetricLevelLtd> results = dataService.getMongoTemplate().aggregate(
                            aggregation,
                            "MetricLevelLtd",
                            MetricLevelLtd.class);

                    resultIterator = results.iterator();
                }

                return resultIterator.hasNext() ? resultIterator.next() : null;
            }
        };
    }


    @Bean
    @StepScope
    public ItemProcessor<MetricLevelLtd, MetricLevelLtd> postAggregationMetricLevelLtdItemProcessor(
            @Value("#{jobParameters['executionDate']}") long executionDate) {
        return new PostAggregationMetricLevelLtdProcessor(executionDate);
    }

    // Item Writer
    @Bean
    public ItemWriter<MetricLevelLtd> postAggregationMetricLevelLtdItemWriter() {
        return items -> {
            if (!items.isEmpty()) {
                Set<MetricLevelLtd> resultSet = new LinkedHashSet<>();
                for (MetricLevelLtd item : items) {
                    resultSet.add(item);
                }
                this.dataService.saveAll(resultSet, MetricLevelLtd.class);
            }
        };
    }
}

