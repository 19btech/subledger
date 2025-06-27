package com.reserv.dataloader.batch.config;

import com.fyntrac.common.entity.AttributeLevelLtd;
import com.fyntrac.common.service.DataService;
import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.PostAggregationAttributeLevelLtdProcessor;
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
public class AttributeLevelPostAggregationConfig {

    @Autowired
    private JobRepository jobRepository;
    private final DataService<AttributeLevelLtd> dataService;
    @Value("${fyntrac.chunk.size}")
    private int chunkSize;
    @Autowired
    public AttributeLevelPostAggregationConfig(DataService<AttributeLevelLtd> dataService) {
        this.dataService = dataService;
    }

    @Bean
    public Step postAggregationAttributeLevelStep() {
        return new StepBuilder("postAggregationAttributeLevelStep", jobRepository)
                .<AttributeLevelLtd, AttributeLevelLtd>chunk(chunkSize, new ResourcelessTransactionManager())
                .reader(attributeLevelPostAggregationReader(0, this.dataService))
                .processor(postAggregationAttributeLevelLtdItemProcessor(0))
                .writer(postAggregationMetricItemWriter())
                .build();
    }


    @Bean("attributeLevelPostAggregationJob")
    public Job attributeLevelPostAggregationJob(JobCompletionNotificationListener listener, Step postAggregationAttributeLevelStep) {
        return new JobBuilder("attributeLevelPostAggregationJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(postAggregationAttributeLevelStep)
                .end()
                .build();
    }


    @Bean
    @StepScope
    public ItemReader<? extends AttributeLevelLtd> attributeLevelPostAggregationReader(
            @Value("#{jobParameters['fromDate']}") long fromDate,
            DataService<AttributeLevelLtd> dataService) {

        return new ItemReader<>() {
            private Iterator<AttributeLevelLtd> resultIterator;

            @Override
            public AttributeLevelLtd read() {
                if (resultIterator == null) {

                    MatchOperation matchStage = Aggregation.match(
                            Criteria.where("postingDate").gte(fromDate));

                    GroupOperation groupStage = Aggregation.group("metricName", "instrumentId", "attributeId")
                            .count().as("count")
                            .first("metricName").as("metricName")
                            .first("instrumentId").as("instrumentId")
                            .first("attributeId").as("attributeId")
                            .first("postingDate").as("postingDate")
                            .first("accountingPeriodId").as("accountingPeriodId")
                            .first("balance").as("balance");

                    MatchOperation havingCountOneStage = Aggregation.match(
                            Criteria.where("count").is(1));

                    Aggregation aggregation = Aggregation.newAggregation(
                            matchStage, groupStage, havingCountOneStage);

                    AggregationResults<AttributeLevelLtd> results = dataService.getMongoTemplate().aggregate(
                            aggregation,
                            "AttributeLevelLtd",
                            AttributeLevelLtd.class);

                    resultIterator = results.iterator();
                }

                return resultIterator.hasNext() ? resultIterator.next() : null;
            }
        };
    }


    @Bean
    @StepScope
    public ItemProcessor<AttributeLevelLtd, AttributeLevelLtd> postAggregationAttributeLevelLtdItemProcessor(
            @Value("#{jobParameters['executionDate']}") long executionDate) {
        return new PostAggregationAttributeLevelLtdProcessor(executionDate);
    }


    // Item Writer
    @Bean
    public ItemWriter<AttributeLevelLtd> postAggregationMetricItemWriter() {
        return items -> {
            if (!items.isEmpty()) {
                Set<AttributeLevelLtd> resultSet = new LinkedHashSet<>();
                for (AttributeLevelLtd item : items) {
                    if(item.getMetricName() == null || item.getMetricName().isEmpty() || item.getMetricName().isBlank()) {
                        continue;
                    }
                    resultSet.add(item);
                }
                this.dataService.getMongoTemplate().insert(resultSet, "AttributeLevelLtd");
            }
        };
    }
}
