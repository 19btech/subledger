package com.fyntrac.common.service;

import com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.Key;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.bson.Document;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TransactionActivityService extends CacheBasedService<TransactionActivity> {
    private final AttributeService attributeService;
    @Value("${fyntrac.chunk.size}")
    private int chunkSize;
    private final TransactionService transactionService;

    @Autowired
    public TransactionActivityService(DataService<TransactionActivity> dataService
                                      , MemcachedRepository memcachedRepository
                                    , AttributeService attributeService
    , TransactionService transactionService) {
        super(dataService, memcachedRepository);
        this.attributeService = attributeService;
        this.transactionService = transactionService;
    }

    @Override
    public TransactionActivity save(TransactionActivity activity) {
        return this.dataService.save(activity);
    }


    @Override
    public Collection<TransactionActivity> fetchAll() {
        return dataService.fetchAllData(TransactionActivity.class);
    }

    @Override
    public void loadIntoCache() throws ExecutionException, InterruptedException {
        ReferenceData referenceData = this.memcachedRepository.getFromCache(this.dataService.getTenantId(), ReferenceData.class);
        int previousAccountingPeriodId = 0;
        if(referenceData != null) {
            previousAccountingPeriodId = referenceData.getPrevioudAccountingPeriodId();

            Set<String> instrumentIds = this.getInstrumentIdsByPeriodId(previousAccountingPeriodId);
            String key = Key.previoudPeriodInstrumentsKey(this.dataService.getTenantId());
            boolean ifExists = this.memcachedRepository.ifExists(key);

            if(ifExists) {
                this.memcachedRepository.delete(key);
             }
            this.memcachedRepository.putInCache(key, instrumentIds);
        }
    }

    public Set<String> getInstrumentIdsByPeriodId(int periodId) {
        Query query = new Query(Criteria.where("periodId").is(periodId));
        List<TransactionActivity> activities = this.dataService.fetchData(query, TransactionActivity.class);
        List<String> instrumentIds = new ArrayList<>();
        for (TransactionActivity ta : activities) {
            instrumentIds.add(ta.getInstrumentId());
        }
        return new HashSet<>(instrumentIds);
    }

    public int getLastAccountingPeriodId(String tenantId) {
        // Execute the query to get the result
        TransactionActivity result = this.get(tenantId,"periodId");

        // Return the periodId or 0 if no result is found
        return result != null ? result.getPeriodId() : 0;
    }

    public Integer getLatestActivityPostingDate(String tenantId) {
        // Execute the query to get the result
        TransactionActivity result = this.get(tenantId, "postingDate");

        // Return the periodId or 0 if no result is found
        return result != null ? result.getPostingDate() : 0;
    }

    public TransactionActivity get(String tenantId, String ... properties) {
        // Fetch the MongoTemplate for the specified tenant
        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate(tenantId);

        // Create a query to find the maximum periodId
        Query query = new Query();
        query.with(Sort.by(Sort.Direction.DESC, properties)); // Sort by periodId in descending order
        query.limit(1); // Limit the result to only one document

        // Execute the query to get the result
        return  mongoTemplate.findOne(query, TransactionActivity.class);
    }
    public Set<String> getColumnNames() {

        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate();
        Set<String> columns = new HashSet<>();

        // Get one document to extract the field names (columns)
        Map<String, Object> doc = mongoTemplate.findOne(Query.query(Criteria.where("_id").exists(true)), Map.class, "TransactionActivity");

        if (doc != null) {
            return doc.keySet();
        }

        return null;
    }

    public List<TransactionActivity> fetchTransactions(List<String> transactionNames, String instrumentId, String attributeId, Date transactionDate ) {
        // Create the query
        Query query = new Query();
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId));
        query.addCriteria(Criteria.where("attributeId").is(attributeId));
        query.addCriteria(Criteria.where("transactionName").in(StringUtil.removeSpaces(transactionNames)));
        query.addCriteria(Criteria.where("postingDate").is(DateUtil.dateInNumber(transactionDate)));// Use range
        // Add sorting by effectiveDate in descending order
        query.with(Sort.by(Sort.Order.desc("effectiveDate")));
        // Execute the query
        return this.dataService.fetchData(query, TransactionActivity.class);
    }


    public Map<String, Object> getReclassableAttributes(Map<String, Object> instrumentAttributes) {
        Map<String, Object> reclassAttributes = new HashMap<>(0);
        Collection<Attributes> attributes = attributeService.getReclassableAttributes();
        for(Attributes attribute : attributes) {
            String attributeName = attribute.getAttributeName();
            Object obj = instrumentAttributes.get(attributeName);
            reclassAttributes.put(attributeName, obj);
        }
        return reclassAttributes;
    }

    public Collection<TransactionActivity> save(Set<TransactionActivity>  transactionActivities) {
        return this.dataService.saveAll(transactionActivities, TransactionActivity.class);
    }

    public Set<TransactionActivity> generateTransactions(List<Map<String, Object>> transactions
            , InstrumentAttribute instrumentAttribute
            , AccountingPeriod accountingPeriod
            , Date executionDate
            , Source source
            , String sourceId) {

        Map<String, Object> attributes = this.getReclassableAttributes(instrumentAttribute.getAttributes());

        Set<TransactionActivity>  transactionActivities = transactions.stream()
                .map(transactionActivityMap -> {
                    try {
                        return this.fillTrascationActivity(transactionActivityMap, accountingPeriod);
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toCollection(HashSet::new));
        transactionActivities.forEach(transactionActivity -> {
            transactionActivity.setAccountingPeriod(accountingPeriod);
            transactionActivity.setOriginalPeriodId(accountingPeriod.getPeriodId());
            try {
                if(transactionActivity.getTransactionDate() != null) {
                    try {
                        transactionActivity.setEffectiveDate(DateUtil.dateInNumber(transactionActivity.getTransactionDate()));
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                }else {
                    transactionActivity.setEffectiveDate(DateUtil.dateInNumber(executionDate));
                }
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            transactionActivity.setPostingDate(transactionActivity.getEffectiveDate());
            if(transactionActivity.getAttributeId() == null || transactionActivity.getAttributeId().isBlank()) {
                transactionActivity.setAttributeId(instrumentAttribute.getAttributeId());
            }
            if(transactionActivity.getInstrumentId() == null || transactionActivity.getInstrumentId().isBlank()) {
                transactionActivity.setInstrumentId(instrumentAttribute.getInstrumentId());
            }

            transactionActivity.setAttributes(attributes);
            transactionActivity.setInstrumentAttributeVersionId(instrumentAttribute.getVersionId());
            transactionActivity.setSource(source);
            transactionActivity.setSourceId(sourceId);

        });
        return transactionActivities;
    }

    public TransactionActivity fillTrascationActivity(Map<String, Object> map, AccountingPeriod accountingPeriod) throws ParseException {
        TransactionActivity.TransactionActivityBuilder builder = TransactionActivity.builder();

        builder.accountingPeriod(accountingPeriod);
        builder.originalPeriodId(accountingPeriod.getPeriodId());
        if (map.containsKey("instrumentId")) {
            builder.instrumentId((String) map.get("instrumentId"));
        }
        if (map.containsKey("instrumentId")) {
            Object instrumentIdObj = map.get("instrumentId");
            if(instrumentIdObj != null) {
                String parseInstrumentId = instrumentIdObj.toString().toUpperCase();
                builder.instrumentId(parseInstrumentId);
            }else{
                builder.instrumentId("");
            }

        }

        if (map.containsKey("transactionName")) {
            Object transactionNameObj = map.get("transactionName");
            if(transactionNameObj != null) {
                String parseTransactionName = transactionNameObj.toString().toUpperCase();
                builder.transactionName(parseTransactionName);
                Transactions transaction = this.transactionService.getTransaction(parseTransactionName.toUpperCase());
                builder.isReplayable((transaction == null) ? 0 : transaction.getIsReplayable());
            }else{
                builder.transactionName("");
            }

        }


        if (map.containsKey("transactionDate")) {
            Object transactionDateObj = map.get("transactionDate");
            if (transactionDateObj != null) {
                String parseTransactionDate = transactionDateObj.toString().trim().toUpperCase();
                if (!parseTransactionDate.isEmpty()) {
                    Date effectiveDate =  DateUtil.convertToUtc(
                            new SimpleDateFormat("M/dd/yyyy").parse(parseTransactionDate)
                    );
                    builder.transactionDate(DateUtil.convertToUtc(
                            new SimpleDateFormat("M/dd/yyyy").parse(parseTransactionDate)));
                    builder.effectiveDate(DateUtil.dateInNumber(effectiveDate));

                }
            }
        }


        if (map.containsKey("amount")) {
            Object amountObj = map.get("amount"); // No need to cast to Object explicitly
            if (amountObj != null) {
                try {
                    // Convert the amount to a String and parse it as a double
                    String amountStr = amountObj.toString(); // Ensure it's a String
                    BigDecimal parsedAmount = BigDecimal.valueOf(Double.valueOf(amountStr));
                    builder.amount(parsedAmount); // Set the amount if parsing succeeds
                } catch (NumberFormatException e) {
                    // Handle the case where the amount is not a valid number
                    log.error("Invalid number format for 'amount': " + amountObj);
                    builder.amount(BigDecimal.valueOf(0.0)); // Set a default value (e.g., 0.0)
                }
            } else {
                // Handle the case where the amount is null
                log.error("Amount is null.");
                builder.amount(BigDecimal.valueOf(0.0d)); // Set a default value (e.g., 0.0)
            }
        }
        if (map.containsKey("attributeId")) {
            Object attributeIdObj = map.get("attributeId");
            if(attributeIdObj !=null) {
                String parseAttributeId = attributeIdObj.toString().toUpperCase();
                builder.attributeId(parseAttributeId);
            }else{
                builder.attributeId("");
            }

        }

        if (map.containsKey("instrumentAttributeVersionId")) {
            Object instrumentAttributeVersionIdObj = map.get("instrumentAttributeVersionId"); // No need to cast to Object explicitly
            if (instrumentAttributeVersionIdObj != null) {
                try {
                    // Convert the amount to a String and parse it as a double
                    String instrumentAttributeVersionIdStr = instrumentAttributeVersionIdObj.toString(); // Ensure it's a String
                    long parsedinstrumentAttributeVersionId = Long.parseLong(instrumentAttributeVersionIdStr);
                    builder.instrumentAttributeVersionId(parsedinstrumentAttributeVersionId); // Set the amount if parsing succeeds
                } catch (NumberFormatException e) {
                    // Handle the case where the amount is not a valid number
                    log.error("Invalid number format for 'instrumentAttributeVersionId': " + instrumentAttributeVersionIdObj);
                    builder.amount(BigDecimal.valueOf(0.0d)); // Set a default value (e.g., 0.0)
                }
            } else {
                // Handle the case where the amount is null
                log.error("instrumentAttributeVersionId is null.");
                builder.instrumentAttributeVersionId(0); // Set a default value (e.g., 0.0)
            }
        }

        if (map.containsKey("batchId")) {
            Object batchIdObj = map.get("batchId"); // No need to cast to Object explicitly
            if (batchIdObj != null) {
                try {
                    // Convert the amount to a String and parse it as a double
                    String batchIdStr = batchIdObj.toString(); // Ensure it's a String
                    long parsedBatchId = Long.parseLong(batchIdStr);
                    builder.batchId(parsedBatchId); // Set the amount if parsing succeeds
                } catch (NumberFormatException e) {
                    // Handle the case where the amount is not a valid number
                    log.error("Invalid number format for 'batchId': " + batchIdObj);
                    builder.amount(BigDecimal.valueOf(0.0d)); // Set a default value (e.g., 0.0)
                }
            } else {
                // Handle the case where the amount is null
                log.error("batchId is null.");
                builder.batchId(0); // Set a default value (e.g., 0.0)
            }
        }

        if (map.containsKey("source")) {
            builder.source((Source) map.get("source"));
        }

        if (map.containsKey("sourceId")) {
            Object sourceIdObj = map.get("sourceId");
            if(sourceIdObj !=null) {
                String parseSourceId = sourceIdObj.toString();
                builder.sourceId(parseSourceId);
            }else{
                builder.sourceId("");
            }

        }

        return builder.build();
    }

    public void updateInstrumentActivityState() {
        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate();
        // Drop the InstrumentActivityState collection to clear existing data
        mongoTemplate.dropCollection(InstrumentActivityState.class);

        long totalRecords = mongoTemplate.count(new Query(), TransactionActivity.class);
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


    public Integer getMaxPostingDate() {
        GroupOperation groupOperation = Aggregation.group().max("postingDate").as("maxPostingDate");

        Aggregation aggregation = Aggregation.newAggregation(groupOperation);

        AggregationResults<Document> results = this.dataService.getMongoTemplate().aggregate(
                aggregation,
                "TransactionActivity", // Collection name
                Document.class
        );

        Document result = results.getUniqueMappedResult();

        return result != null ? result.getInteger("maxPostingDate") : null;
    }

    public Integer getPreviousMaxPostingDate(Long postingDate) {
        MatchOperation matchOperation = Aggregation.match(Criteria.where("postingDate").lt(postingDate));

        GroupOperation groupOperation = Aggregation.group().max("postingDate").as("maxPostingDate");

        Aggregation aggregation = Aggregation.newAggregation(
                matchOperation,
                groupOperation
        );

        AggregationResults<Document> results = this.dataService.getMongoTemplate().aggregate(
                aggregation,
                "TransactionActivity", // your collection name
                Document.class
        );

        Document result = results.getUniqueMappedResult();
        return result != null ? result.getInteger("maxPostingDate") : null;
    }


}
