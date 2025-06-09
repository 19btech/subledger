package com.fyntrac.common.service;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

@Service
public class TransactionActivityReversalService {

    private final DataService dataService;

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
        String attributeId = activity.getAttributeId();
        String transactionName = activity.getTransactionName().trim()
                .replaceAll("[\\u2013\\u2014]", "-") // Replace en/em dashes with hyphen
                .replaceAll("\\s+", " ")            // Normalize whitespace
                .toUpperCase();

        Criteria criteria = Criteria.where("effectiveDate").gte(effectiveDate)
                .and("instrumentId").is(instrumentId)
                .and("attributeId").is(attributeId); // Make sure this is a string in your DB
        Query query = new Query(criteria);
        // Execute the aggregation
        Collection<TransactionActivity> results = this.dataService.fetchData(query, TransactionActivity.class);


        return results;
    }


    public Collection<Records.TransactionActivityReversalRecord> getGroupTransactionSummary(TransactionActivity activity) {
        // Create the aggregation pipeline

//        Aggregation aggregation = Aggregation.newAggregation(
//                Aggregation.match(
//                        Criteria.where("effectiveDate").gte(20241025)
//                                .and("transactionName").is("PURCHASE - UPB")
//                                .and("instrumentId").is("ZXUB-ST2R")
//                                .and("attributeId").is("1.0") // Make sure this is a string in your DB
//                ),
//                Aggregation.group("instrumentId", "attributeId", "transactionName")
//                        .sum("amount").as("totalAmount"),
//                Aggregation.project()
//                        .and("_id.instrumentId").as("instrumentId")
//                        .and("_id.attributeId").as("attributeId")
//                        .and("_id.transactionName").as("transactionType")
//                        .and("totalAmount").as("totalAmount")
//        );

        int effectiveDate = activity.getEffectiveDate();
        String instrumentId = activity.getInstrumentId();
        String attributeId = activity.getAttributeId();
        String transactionName = activity.getTransactionName().trim()
                .replaceAll("[\\u2013\\u2014]", "-") // Replace en/em dashes with hyphen
                .replaceAll("\\s+", " ")            // Normalize whitespace
                .toUpperCase();

        Criteria criteria = Criteria.where("effectiveDate").gte(effectiveDate)
                .and("postingDate").lt(activity.getPostingDate())
                .and("instrumentId").is(instrumentId)
                .and("attributeId").is(attributeId); // Make sure this is a string in your DB

        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(
                        criteria),
                Aggregation.group("instrumentId", "attributeId", "transactionName", "effectiveDate")
                        .sum("amount").as("totalAmount"),
                Aggregation.project()
                        .and("_id.instrumentId").as("instrumentId")
                        .and("_id.attributeId").as("attributeId")
                        .and("_id.transactionName").as("transactionType")
                        .and("_id.effectiveDate").as("effectiveDate")
                        .and("totalAmount").as("totalAmount")
        );

        // Execute the aggregation
        AggregationResults<Records.TransactionActivityReversalRecord> results = this.dataService.getMongoTemplate().aggregate(
                aggregation,
                "TransactionActivity", // your collection name
                Records.TransactionActivityReversalRecord.class
        );

        List<Records.TransactionActivityReversalRecord> mappedResults = results.getMappedResults();

        if (mappedResults.isEmpty()) {
            return null; // or throw an exception if you prefer
        }

        return mappedResults;
    }

    public TransactionActivity generateTransactionReversalActivity(Records.TransactionActivityReversalRecord reversalRecord, TransactionActivity activity) {

        if (reversalRecord == null) {
            throw new IllegalArgumentException("No reversal record found in the results.");
        }

        return TransactionActivity.builder()
                .accountingPeriod(activity.getAccountingPeriod())
                .postingDate(activity.getPostingDate())
                .instrumentId(activity.getInstrumentId()) // Use getter method
                .attributeId(activity.getAttributeId()) // Use getter method
                .transactionName(activity.getTransactionName()) // Use getter method
                .originalPeriodId(activity.getOriginalPeriodId()) // Use getter method
                .attributes(activity.getAttributes())
                .transactionDate(DateUtil.convertToDateFromYYYYMMDD(activity.getPostingDate()))
                .source(Source.REVERSAL)
                .instrumentAttributeVersionId(activity.getInstrumentAttributeVersionId())
                .amount(reversalRecord.totalAmount().negate())
                .batchId(activity.getBatchId())
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
                    .transactionDate(DateUtil.convertToDateFromYYYYMMDD(ta.effectiveDate()))
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
                    .transactionDate(DateUtil.convertToDateFromYYYYMMDD(ta.getEffectiveDate()))
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
