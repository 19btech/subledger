package com.reserv.dataloader.batch.tasklet;

import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.entity.InstrumentReplayState;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.utils.DateUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReplayStateComputationTasklet implements Tasklet {

    private final TenantDataSourceProvider dataSourceProvider;
    private final ExecutionStateService executionStateService;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        String tenantId = (String) chunkContext.getStepContext().getJobParameters().get("tenantId");

        // 1. Get Execution Date
        ExecutionState executionState = executionStateService.fetchLatest();
        if (executionState == null) {
            log.warn("No ExecutionState found. Skipping Replay Calculation.");
            return RepeatStatus.FINISHED;
        }

        Integer executionDate = executionState.getExecutionDate();
        Integer lastExecutionDate = executionState.getLastExecutionDate();
        log.info("Calculating Replay State for Execution Date: {}", executionDate);

        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);
        if (mongoTemplate == null || lastExecutionDate == 0) return RepeatStatus.FINISHED;

        // 2. Aggregate InstrumentAttribute
        List<InstrumentReplayState> attributeCandidates = runAttributeAggregation(
                mongoTemplate,
                "InstrumentAttribute",
                executionDate,
               lastExecutionDate
        );

        // 3. Aggregate TransactionActivity
        List<InstrumentReplayState> activityCandidates = runAggregation(
                mongoTemplate,
                "TransactionActivity",
                executionDate,
                lastExecutionDate
        );

        // 4. Upsert Results (Update MinEffectiveDate if new one is lower)
        bulkUpsert(mongoTemplate, attributeCandidates, executionDate);
        bulkUpsert(mongoTemplate, activityCandidates, executionDate);

        return RepeatStatus.FINISHED;
    }

    private List<InstrumentReplayState> runAttributeAggregation(MongoTemplate mongoTemplate, String collectionName,
                                                       Integer executionDate, Integer lastExecutionDate) {
        /*
         * PIPELINE:
         * 1. MATCH: postingDate == executionDate AND effectiveDate < executionDate
         * 2. GROUP: by instrumentId, attributeId. MIN(effectiveDate)
         * 3. PROJECT: Map to InstrumentReplayState class
         */
        Aggregation aggregation = newAggregation(
                match(Criteria.where("postingDate").is(executionDate)
                        .and("intEffectiveDate").lt(lastExecutionDate)), // DateUtil conversion might be needed if
                // stored as Date

                group("instrumentId", "attributeId")
                        .min("intEffectiveDate").as("minEffectiveDate")
                        .first("instrumentId").as("instrumentId")
                        .first("attributeId").as("attributeId"),

                project("instrumentId", "attributeId", "minEffectiveDate")
        );

        AggregationResults<InstrumentReplayState> results = mongoTemplate.aggregate(
                aggregation, collectionName, InstrumentReplayState.class
        );

        log.info("Found {} candidates in {}", results.getMappedResults().size(), collectionName);
        return results.getMappedResults();
    }

    private List<InstrumentReplayState> runAggregation(MongoTemplate mongoTemplate, String collectionName,
                                                       Integer executionDate, Integer lastExecutionDate) {
        /*
         * PIPELINE:
         * 1. MATCH: postingDate == executionDate AND effectiveDate < executionDate
         * 2. GROUP: by instrumentId, attributeId. MIN(effectiveDate)
         * 3. PROJECT: Map to InstrumentReplayState class
         */
        Aggregation aggregation = newAggregation(
                match(Criteria.where("postingDate").is(executionDate)
                        .and("effectiveDate").lt(lastExecutionDate)), // DateUtil conversion might be needed if
                // stored as Date

                group("instrumentId", "attributeId")
                        .min("effectiveDate").as("minEffectiveDate")
                        .first("instrumentId").as("instrumentId")
                        .first("attributeId").as("attributeId"),

                project("instrumentId", "attributeId", "minEffectiveDate")
        );

        AggregationResults<InstrumentReplayState> results = mongoTemplate.aggregate(
                aggregation, collectionName, InstrumentReplayState.class
        );

        log.info("Found {} candidates in {}", results.getMappedResults().size(), collectionName);
        return results.getMappedResults();
    }

    private void bulkUpsert(MongoTemplate mongoTemplate, List<InstrumentReplayState> candidates, Integer maxPostingDate) {
        if (candidates.isEmpty()) return;

        BulkOperations bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, InstrumentReplayState.class);

        for (InstrumentReplayState candidate : candidates) {
            // ID Strategy: Composite key to ensure uniqueness
            String id = candidate.getInstrumentId() + "_" + (candidate.getAttributeId() == null ? "NA" : candidate.getAttributeId());

            Query query = new Query(Criteria.where("_id").is(id));

            Update update = new Update()
                    .setOnInsert("instrumentId", candidate.getInstrumentId())
                    .setOnInsert("attributeId", candidate.getAttributeId())
                    .set("maxPostingDate", maxPostingDate)
                    // CRITICAL: Only update minEffectiveDate if the new value is LOWER than existing
                    .min("minEffectiveDate", candidate.getMinEffectiveDate());

            bulkOps.upsert(query, update);
        }

        bulkOps.execute();
    }
}