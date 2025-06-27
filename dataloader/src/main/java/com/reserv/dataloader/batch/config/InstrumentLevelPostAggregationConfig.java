package com.reserv.dataloader.batch.config;

import com.fyntrac.common.entity.InstrumentLevelLtd;
import com.fyntrac.common.service.DataService;
import com.reserv.dataloader.batch.listener.JobCompletionNotificationListener;
import com.reserv.dataloader.batch.processor.PostAggregationInstrumentLevelLtdProcessor;
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
public class InstrumentLevelPostAggregationConfig {

    @Autowired
    private JobRepository jobRepository;
    private final DataService<InstrumentLevelLtd> dataService;
    @Value("${fyntrac.chunk.size}")
    private int chunkSize;

    @Autowired
    public InstrumentLevelPostAggregationConfig(DataService<InstrumentLevelLtd> dataService) {
        this.dataService = dataService;
    }

    @Bean("instrumentLevelPostAggregationJob")
    public Job instrumentLevelPostAggregationJob(JobCompletionNotificationListener listener, Step postAggregationInstrumentLevelStep) {
        return new JobBuilder("instrumentLevelPostAggregationJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(postAggregationInstrumentLevelStep)
                .end()
                .build();
    }

    @Bean
    public Step postAggregationInstrumentLevelStep() {
        return new StepBuilder("postAggregationInstrumentLevelStep", jobRepository)
                .<InstrumentLevelLtd, InstrumentLevelLtd>chunk(chunkSize, new ResourcelessTransactionManager())
                .reader(instrumentLevelPostAggregationReader(0, this.dataService))
                .processor(postAggregationInstrumentLevelLtdItemProcessor(0))
                .writer(postAggregationInstrumentLevelLtdItemWriter())
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<? extends InstrumentLevelLtd> instrumentLevelPostAggregationReader(
            @Value("#{jobParameters['fromDate']}") long fromDate,
            DataService<InstrumentLevelLtd> dataService) {

        return new ItemReader<>() {
            private Iterator<InstrumentLevelLtd> resultIterator;

            @Override
            public InstrumentLevelLtd read() {
                if (resultIterator == null) {

                    MatchOperation matchStage = Aggregation.match(
                            Criteria.where("postingDate").gte(fromDate));

                    GroupOperation groupStage = Aggregation.group("metricName", "instrumentId")
                            .count().as("count")
                            .first("metricName").as("metricName")
                            .first("instrumentId").as("instrumentId")
                            .first("postingDate").as("postingDate")
                            .first("accountingPeriodId").as("accountingPeriodId")
                            .first("balance").as("balance");

                    MatchOperation havingCountOneStage = Aggregation.match(
                            Criteria.where("count").is(1));

                    Aggregation aggregation = Aggregation.newAggregation(
                            matchStage, groupStage, havingCountOneStage);

                    AggregationResults<InstrumentLevelLtd> results = dataService.getMongoTemplate().aggregate(
                            aggregation,
                            "InstrumentLevelLtd",
                            InstrumentLevelLtd.class);

                    resultIterator = results.iterator();
                }

                return resultIterator.hasNext() ? resultIterator.next() : null;
            }
        };
    }


    @Bean
    @StepScope
    public ItemProcessor<InstrumentLevelLtd, InstrumentLevelLtd> postAggregationInstrumentLevelLtdItemProcessor(
            @Value("#{jobParameters['executionDate']}") long executionDate) {
        return new PostAggregationInstrumentLevelLtdProcessor(executionDate);
    }

    // Item Writer
    @Bean
    public ItemWriter<InstrumentLevelLtd> postAggregationInstrumentLevelLtdItemWriter() {
        return items -> {
            if (!items.isEmpty()) {
                Set<InstrumentLevelLtd> resultSet = new LinkedHashSet<>();
                for (InstrumentLevelLtd item : items) {
                    if(item.getMetricName() == null || item.getMetricName().isEmpty() || item.getMetricName().isBlank()) {
                        continue;
                    }
                    resultSet.add(item);
                }
                this.dataService.getMongoTemplate().insert(resultSet, "InstrumentLevelLtd");
            }
        };
    }
}
