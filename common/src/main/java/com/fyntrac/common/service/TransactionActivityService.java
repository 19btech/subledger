package com.fyntrac.common.service;

import com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TransactionActivityService extends CacheBasedService<TransactionActivity> {
    private AttributeService attributeService;
    @Autowired
    public TransactionActivityService(DataService<TransactionActivity> dataService
                                      , MemcachedRepository memcachedRepository
                                    , AttributeService attributeService) {
        super(dataService, memcachedRepository);
        this.attributeService = attributeService;
    }

    @Override
    public void save(TransactionActivity activity) {
        this.dataService.save(activity);
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
        // Fetch the MongoTemplate for the specified tenant
        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate(tenantId);

        // Create a query to find the maximum periodId
        Query query = new Query();
        query.with(Sort.by(Sort.Direction.DESC, "periodId")); // Sort by periodId in descending order
        query.limit(1); // Limit the result to only one document

        // Execute the query to get the result
        TransactionActivity result = mongoTemplate.findOne(query, TransactionActivity.class);

        // Return the periodId or 0 if no result is found
        return result != null ? result.getPeriodId() : 0;
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

    public List<TransactionActivity> fetchTransactions(List<String> transactionNames, String instrumentId, String attributeId, Date transactionDate) {
        // Define the date format
        Date endDate;

        // Parse the input date string
        // Set the end date to the start of the next day
        endDate = new Date(transactionDate.getTime() + (1000 * 60 * 60 * 24)); // Add 1 day

        // Create the query
        Query query = new Query();
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId));
        query.addCriteria(Criteria.where("attributeId").is(attributeId));
        query.addCriteria(Criteria.where("transactionName").in(transactionNames));
        query.addCriteria(Criteria.where("transactionDate").gte(transactionDate).lt(endDate)); // Use range

        // Execute the query
        return this.dataService.fetchData(query, TransactionActivity.class);
    }


    private Map<String, Object> getReclassableAttributes(Map<String, Object> instrumentAttributes) {
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
            , Date transactionDate
            , Source source
            , String sourceId) {

        Map<String, Object> attributes = this.getReclassableAttributes(instrumentAttribute.getAttributes());

        Set<TransactionActivity>  transactionActivities = transactions.stream()
                .map(transactionActivityMap -> this.save(transactionActivityMap, accountingPeriod))
                .collect(Collectors.toCollection(HashSet::new));

        transactionActivities.forEach(transactionActivity -> {
            transactionActivity.setAccountingPeriod(accountingPeriod);
            transactionActivity.setOriginalPeriodId(accountingPeriod.getPeriodId());
            transactionActivity.setTransactionDate(DateUtil.stripTime(transactionDate));
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

    public TransactionActivity save(Map<String, Object> map, AccountingPeriod accountingPeriod) {
        TransactionActivity.TransactionActivityBuilder builder = TransactionActivity.builder();

        builder.accountingPeriod(accountingPeriod);
        builder.originalPeriodId(accountingPeriod.getPeriodId());
        if (map.containsKey("instrumentId")) {
            builder.instrumentId((String) map.get("instrumentId"));
        }
        if (map.containsKey("instrumentId")) {
            Object instrumentIdObj = map.get("instrumentId");
            if(instrumentIdObj != null) {
                String parseInstrumentId = instrumentIdObj.toString();
                builder.instrumentId(parseInstrumentId);
            }else{
                builder.instrumentId("");
            }

        }

        if (map.containsKey("transactionName")) {
            Object transactionNameObj = map.get("transactionName");
            if(transactionNameObj != null) {
                String parseTransactionName = transactionNameObj.toString();
                builder.transactionName(parseTransactionName);
            }else{
                builder.transactionName("");
            }

        }
        if (map.containsKey("amount")) {
            Object amountObj = map.get("amount"); // No need to cast to Object explicitly
            if (amountObj != null) {
                try {
                    // Convert the amount to a String and parse it as a double
                    String amountStr = amountObj.toString(); // Ensure it's a String
                    double parsedAmount = Double.parseDouble(amountStr);
                    builder.amount(parsedAmount); // Set the amount if parsing succeeds
                } catch (NumberFormatException e) {
                    // Handle the case where the amount is not a valid number
                    log.error("Invalid number format for 'amount': " + amountObj);
                    builder.amount(0.0d); // Set a default value (e.g., 0.0)
                }
            } else {
                // Handle the case where the amount is null
                log.error("Amount is null.");
                builder.amount(0.0d); // Set a default value (e.g., 0.0)
            }
        }
        if (map.containsKey("attributeId")) {
            Object attributeIdObj = map.get("attributeId");
            if(attributeIdObj !=null) {
                String parseAttributeId = attributeIdObj.toString();
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
                    builder.amount(0.0d); // Set a default value (e.g., 0.0)
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
                    builder.amount(0.0d); // Set a default value (e.g., 0.0)
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
}
