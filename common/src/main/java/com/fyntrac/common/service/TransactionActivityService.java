package com.fyntrac.common.service;

import com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class TransactionActivityService extends CacheBasedService<TransactionActivity> {
    @Autowired
    public TransactionActivityService(DataService<TransactionActivity> dataService
                                      , MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
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
        // Define the query
        Query query = new Query();

        // Add criteria to filter by transactionName (list) and transactionDate
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId)
                .and("attributeId").is(attributeId)
                .and("transactionName").in(transactionNames)
                .and("transactionDate").is(transactionDate));

        // Execute the query
        return this.dataService.fetchData(query, TransactionActivity.class);
    }
}
