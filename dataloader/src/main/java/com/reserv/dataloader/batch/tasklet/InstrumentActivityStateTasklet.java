package com.reserv.dataloader.batch.tasklet;

import com.fyntrac.common.entity.InstrumentActivityReplayState;
import com.fyntrac.common.entity.InstrumentActivityState;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.service.TransactionActivityService;
import com.fyntrac.common.utils.StringUtil;
import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import org.bson.conversions.Bson;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class InstrumentActivityStateTasklet implements Tasklet {

    private final TransactionActivityService activityService;

    @Value("${fyntrac.chunk.size}")
    private int chunkSize;

    @Autowired
    public InstrumentActivityStateTasklet(TransactionActivityService activityService) {
        this.activityService = activityService;
    }
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        try {
            // Retrieve the JobParameters
            JobParameters jobParameters = contribution.getStepExecution().getJobParameters();

            // Check if jobParameters and the execution-date parameter are not null
            if (jobParameters != null && jobParameters.getLong("execution-date") != null) {
                long executionDate = jobParameters.getLong("execution-date");
                this.updateInstrumentActivityState();
                this.updateInstrumentActivityReplayState(executionDate);
            } else {
                log.error("Execution date parameter is missing or null.");
                // Handle the case where execution-date is not present
                // You can throw an exception or return an error status if needed
            }
        } catch (Exception exception) {
            log.error(String.format("Failed: updateInstrumentActivityState %s", StringUtil.getStackTrace(exception)));
        }
        return RepeatStatus.FINISHED;
    }


    public void updateInstrumentActivityState() {
        MongoTemplate mongoTemplate = this.activityService.getDataService().getMongoTemplate();
        // Drop the InstrumentActivityState collection to clear existing data
        mongoTemplate.dropCollection(InstrumentActivityState.class);

        long totalRecords = mongoTemplate.getCollection("TransactionActivity")
                .distinct("instrumentId", String.class)
                .into(new ArrayList<>

())
                .size();
        long processedRecords = 0;

        while (processedRecords < totalRecords) {
            GroupOperation group = Aggregation.group("instrumentId", "attributeId")
                    .max("effectiveDate").as("maxTransactionDate");

            ProjectionOperation project = Aggregation.project()
                    .and("_id.instrumentId").as("instrumentId")
                    .and("_id.attributeId").as("attributeId")
                    .and("maxTransactionDate").as("maxTransactionDate")
                    .andExclude("_id");

            Aggregation aggregation = Aggregation.newAggregation(
                    group,
                    project,
                    Aggregation.skip(processedRecords),
                    Aggregation.limit(chunkSize)
            );

            AggregationResults<InstrumentActivityState> results = mongoTemplate.aggregate(aggregation,
                    TransactionActivity.class,
                    InstrumentActivityState.class);

            List<InstrumentActivityState> maxDates = results.getMappedResults();

            if (!maxDates.isEmpty()) {
                mongoTemplate.insertAll(maxDates);
            }

            processedRecords += maxDates.size();
        }

    }

    public void updateInstrumentActivityReplayState(long postingDate) {
        MongoTemplate mongoTemplate = this.activityService.getDataService().getMongoTemplate();
        // Drop the InstrumentActivityState collection to clear existing data
        mongoTemplate.dropCollection(InstrumentActivityReplayState.class);

        Bson filter = Filters.eq("postingDate", postingDate);

        long totalRecords = mongoTemplate.getCollection("TransactionActivity")
                .distinct("instrumentId", filter, String.class)
                .into(new ArrayList<>

                        ())
                .size();
        long processedRecords = 0;

        while (processedRecords < totalRecords) {
            MatchOperation match = Aggregation.match(Criteria.where("postingDate").is(postingDate));
            GroupOperation group = Aggregation.group("instrumentId", "attributeId")
                    .min("effectiveDate").as("minTransactionDate");

            ProjectionOperation project = Aggregation.project()
                    .and("_id.instrumentId").as("instrumentId")
                    .and("_id.attributeId").as("attributeId")
                    .and("minTransactionDate").as("minTransactionDate")
                    .andExclude("_id");

            Aggregation aggregation = Aggregation.newAggregation(
                    match,
                    group,
                    project,
                    Aggregation.skip(processedRecords),
                    Aggregation.limit(chunkSize)
            );

            AggregationResults<InstrumentActivityReplayState> results = mongoTemplate.aggregate(aggregation,
                    TransactionActivity.class,
                    InstrumentActivityReplayState.class);

            List<InstrumentActivityReplayState> maxDates = results.getMappedResults();

            if (!maxDates.isEmpty()) {
                mongoTemplate.insertAll(maxDates);
            }

            processedRecords += maxDates.size();
        }
    }
}
