package com.fyntrac.common.service;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.NumberUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

@Service
public class TransactionActivityReversalService {

    private final DataService dataService;

    public DataService getDataService() {
        return this.dataService;
    }
    @Autowired
    public TransactionActivityReversalService(DataService dataService) {
        this.dataService = dataService;
      }

    public Collection<TransactionActivity> bookReversal(TransactionActivity activity) {
            Collection<TransactionActivity> reversalRecord =  this.getTransactionSummary(activity);
           return this.generateTransactionReversalActivity(reversalRecord, activity);
    }

    public Collection<TransactionActivity> getTransactionSummary(TransactionActivity activity) {
        int effectiveDate = activity.getEffectiveDate();
        String instrumentId = activity.getInstrumentId();

        Criteria criteria = Criteria.where("effectiveDate").gte(effectiveDate)
                .and("instrumentId").is(instrumentId);

        MatchOperation match = Aggregation.match(criteria);

        GroupOperation group = Aggregation.group("effectiveDate", "instrumentId", "attributeId")
                .first("effectiveDate").as("effectiveDate")
                .first("instrumentId").as("instrumentId")
                .first("attributeId").as("attributeId")
                .first("transactionName").as("transactionName")
                .first("amount").as("amount")
                .first("attributes").as("attribute")
                .first("batchId").as("batchId")
                .first("instrumentAttributeVersionId").as("instrumentAttributeVersionId")
                .first("transactionDate").as("transactionDate")
                .first("originalPeriodId").as("originalPeriodId");// Replace/add all required fields

        ProjectionOperation project = Aggregation.project(TransactionActivity.class);

        Aggregation aggregation = Aggregation.newAggregation(match, group, project);

        AggregationResults<TransactionActivity> results =
                this.dataService.getMongoTemplate().aggregate(aggregation, "transactionActivity", TransactionActivity.class);

        return results.getMappedResults();
    }


    public Collection<Records.TransactionActivityReversalRecord> getGroupTransactionSummary(
            String instrumentId, int postingDate, int effectiveDate) {

        Criteria criteria = Criteria.where("effectiveDate").gte(effectiveDate)
                .and("postingDate").lt(postingDate)
                .and("instrumentId").is(instrumentId)
                .and("isReplayable").is(1);

        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(criteria),
                Aggregation.group("instrumentId", "attributeId", "transactionName", "effectiveDate")
                        .sum(ConvertOperators.ToDouble.toDouble("$amount")).as("totalAmount")
                        .last("attributes").as("attributes")
                        .last("originalPeriodId").as("originalPeriodId")
                        .last("instrumentAttributeVersionId").as("instrumentAttributeVersionId")
                        .last("transactionDate").as("transactionDate")
                        .last("accountingPeriod").as("accountingPeriod")
                        .last("batchId").as("batchId"),
                Aggregation.project()
                        .and("_id.instrumentId").as("instrumentId")
                        .and("_id.attributeId").as("attributeId")
                        .and("_id.transactionName").as("transactionType")
                        .and("_id.effectiveDate").as("effectiveDate")
                        .and("totalAmount").as("totalAmount")
                        .and("attributes").as("attributes")
                        .and("instrumentAttributeVersionId").as("instrumentAttributeVersionId")
                        .and("originalPeriodId").as("originalPeriodId")
                        .and("transactionDate").as("transactionDate")
                        .and("accountingPeriod").as("accountingPeriod")
                        .and("batchId").as("batchId")
        );

        AggregationResults<Records.TransactionActivityReversalRecord> results =
                this.dataService.getMongoTemplate()
                        .aggregate(aggregation, "TransactionActivity", Records.TransactionActivityReversalRecord.class);

        List<Records.TransactionActivityReversalRecord> mappedResults = results.getMappedResults();
        return mappedResults.isEmpty() ? List.of() : mappedResults;
    }


    public TransactionActivity generateTransactionReversalActivity(Records.TransactionActivityReversalRecord reversalRecord, TransactionActivity activity) throws ParseException {

        if (reversalRecord == null) {
            throw new IllegalArgumentException("No reversal record found in the results.");
        }

        return TransactionActivity.builder()
                .accountingPeriod(activity.getAccountingPeriod())
                .postingDate(activity.getPostingDate())
                .effectiveDate(activity.getEffectiveDate())
                .instrumentId(activity.getInstrumentId()) // Use getter method
                .attributeId(activity.getAttributeId()) // Use getter method
                .transactionName(activity.getTransactionName()) // Use getter method
                .originalPeriodId(activity.getOriginalPeriodId()) // Use getter method
                .attributes(activity.getAttributes())
                .source(Source.REVERSAL)
                .instrumentAttributeVersionId(activity.getInstrumentAttributeVersionId())
                .amount(NumberUtil.getNumber(reversalRecord.totalAmount()).negate())
                .batchId(activity.getBatchId())
                .build();
    }

    public TransactionActivity generateTransactionReversalActivity(Records.TransactionActivityReversalRecord reversalRecord, Records.InstrumentReplayRecord activity) {

        if (reversalRecord == null) {
            throw new IllegalArgumentException("No reversal record found in the results.");
        }

        return TransactionActivity.builder()
                .accountingPeriod(reversalRecord.accountingPeriod())
                .postingDate(activity.postingDate())
                .effectiveDate(reversalRecord.effectiveDate())
                .instrumentId(reversalRecord.instrumentId()) // Use getter method
                .attributeId(reversalRecord.attributeId()) // Use getter method
                .transactionName(reversalRecord.transactionType()) // Use getter method
                .originalPeriodId(reversalRecord.originalPeriodId()) // Use getter method
                .attributes(reversalRecord.attributes())
                .source(Source.REVERSAL)
                .instrumentAttributeVersionId(reversalRecord.instrumentAttributeVersionId())
                .amount(NumberUtil.getNumber(reversalRecord.totalAmount()).negate())
                .batchId(reversalRecord.batchId())
                .build();
    }

    public List<TransactionActivity> generateTGroupransactionReversalActivity(Collection<Records.TransactionActivityReversalRecord> reversalActivities, TransactionActivity activity) {

        if (reversalActivities == null || reversalActivities.isEmpty()) {
            throw new IllegalArgumentException("No reversal record found in the results.");
        }

        List<TransactionActivity> reversals = new ArrayList<>(0);

        for(Records.TransactionActivityReversalRecord ta :  reversalActivities) {
            TransactionActivity transactionActivity =  TransactionActivity.builder()
                    .accountingPeriod(activity.getAccountingPeriod())
                    .postingDate(activity.getPostingDate())
                    .effectiveDate(ta.effectiveDate())
                    .instrumentId(activity.getInstrumentId()) // Use getter method
                    .attributeId(activity.getAttributeId()) // Use getter method
                    .transactionName(ta.transactionType()) // Use getter method
                    .originalPeriodId(activity.getOriginalPeriodId()) // Use getter method
                    .attributes(activity.getAttributes())
                    .source(Source.REVERSAL)
                    .instrumentAttributeVersionId(activity.getInstrumentAttributeVersionId())
                    .amount(ta.totalAmount().negate())
                    .batchId(activity.getBatchId())
                    .build();
            reversals.add(transactionActivity);
        }
        return reversals;
    }

    public Collection<TransactionActivity> generateTransactionReversalActivity(Collection<TransactionActivity> reversalActivities, TransactionActivity activity) {

        if (reversalActivities == null || reversalActivities.isEmpty()) {
            throw new IllegalArgumentException("No reversal record found in the results.");
        }

        Collection<TransactionActivity> reversalEntries = new HashSet<>(0);
        for(TransactionActivity ta :  reversalActivities) {
            TransactionActivity reversalEntry =  TransactionActivity.builder()
                    .accountingPeriod(activity.getAccountingPeriod())
                    .postingDate(activity.getPostingDate())
                    .effectiveDate(ta.getEffectiveDate())
                    .instrumentId(activity.getInstrumentId()) // Use getter method
                    .attributeId(activity.getAttributeId()) // Use getter method
                    .transactionName(ta.getTransactionName()) // Use getter method
                    .originalPeriodId(activity.getOriginalPeriodId()) // Use getter method
                    .attributes(activity.getAttributes())
                    .source(Source.REVERSAL)
                    .instrumentAttributeVersionId(activity.getInstrumentAttributeVersionId())
                    .amount(ta.getAmount().negate())
                    .batchId(activity.getBatchId())
                    .build();
                    reversalEntries.add(reversalEntry);
        }
        return reversalEntries;
    }
}
