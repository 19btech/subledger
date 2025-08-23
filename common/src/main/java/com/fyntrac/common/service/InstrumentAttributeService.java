package com.fyntrac.common.service;

import com.fyntrac.common.cache.collection.CacheMap;
import  com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.entity.InstrumentActivityState;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.entity.factory.InstrumentAttributeFactory;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.repository.InstrumentAttributeRepository;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
// import org.bson.Document;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class InstrumentAttributeService extends CacheBasedService<InstrumentAttribute> {

    private final InstrumentAttributeRepository instrumentAttributeRepository;
    private final InstrumentAttributeFactory instrumentAttributeFactory;

    @Autowired
    public InstrumentAttributeService(DataService<InstrumentAttribute> dataService
            , MemcachedRepository memcachedRepository
            , InstrumentAttributeRepository instrumentAttributeRepository
            , InstrumentAttributeFactory instrumentAttributeFactory) {
        super(dataService, memcachedRepository);
        this.instrumentAttributeRepository = instrumentAttributeRepository;
        this.instrumentAttributeFactory = instrumentAttributeFactory;
    }

    public List<InstrumentAttribute> getLastOpenInstrumentAttributes(String instrumentId, String attributeId) {
        return instrumentAttributeRepository.findByAttributeIdAndInstrumentIdAndEndDateIsNull(attributeId, instrumentId);
    }

    public InstrumentAttribute getLastOpenInstrumentAttributes(String instrumentId, String attributeId, Integer postingDate, String tenantId) {

        Query query = new Query();
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId));
        query.addCriteria(Criteria.where("attributeId").is(attributeId));
        query.addCriteria(Criteria.where("postingDate").lt(postingDate));
        // Sort by versionId descending to get the latest
        query.with(Sort.by(Sort.Direction.DESC, "versionId"));

        // Limit to 1 document
        query.limit(1);

        InstrumentAttribute instrumentAttribute = this.dataService.findOne(query, tenantId, InstrumentAttribute.class);
        return instrumentAttribute;
    }


    // Define a method in your service class
    public List<InstrumentAttribute> getOpenInstrumentAttributes(String attributeId, String instrumentId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("attributeId").is(attributeId));
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId));
        query.addCriteria(Criteria.where("endDate").is(null));

        return this.dataService.fetchData(query, InstrumentAttribute.class);
    }

    public InstrumentAttribute getFirstVersionOfInstrumentAttributes(String instrumentId, String attributeId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("attributeId").is(attributeId));
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId));
        query.addCriteria(Criteria.where("previousVersionId").is(0));

        return this.dataService.findOne(query, InstrumentAttribute.class);
    }

    // Define a method in your service class
    public List<InstrumentAttribute> getOpenInstrumentAttributes(String attributeId, String instrumentId, String tenantId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("attributeId").is(attributeId));
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId));
        query.addCriteria(Criteria.where("endDate").is(null));

        return this.dataService.fetchData(query, tenantId, InstrumentAttribute.class);
    }

    // Define a method in your service class
    public List<InstrumentAttribute> getOpenInstrumentAttributesByInstrumentId(String instrumentId, String tenantId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId));
        query.addCriteria(Criteria.where("endDate").is(null));

        return this.dataService.fetchData(query, tenantId, InstrumentAttribute.class);
    }


    public List<InstrumentAttribute> getOpenInstrumentAttributesByInstrumentId(String instrumentId, String modelId, Integer posltingDate, String tenantId) {
        // Match documents with the specified instrumentId and source not equal to modelId
        MatchOperation matchStage = Aggregation.match(
                Criteria.where("instrumentId").is(instrumentId)
                        .and("postingDate").lte(posltingDate)
                        .and("sourceId").ne(modelId)  // Changed from sourceId to source
        );

        // Sort by attributeId (ascending) and versionId (descending)
        SortOperation sortStage = Aggregation.sort(
                Sort.by(Sort.Order.asc("attributeId"),
                        Sort.Order.desc("versionId"))
        );

        // Group by attributeId and get the first (latest) document for each group
        GroupOperation groupStage = Aggregation.group("attributeId")
                .first(Aggregation.ROOT).as("latest");

        // Replace the root with the latest document
        ReplaceRootOperation replaceRoot = Aggregation.replaceRoot("latest");

        Aggregation aggregation = Aggregation.newAggregation(
                matchStage, sortStage, groupStage, replaceRoot
        );

        return this.dataService.getMongoTemplate(tenantId)
                .aggregate(aggregation, "InstrumentAttribute", InstrumentAttribute.class)
                .getMappedResults();
    }
    // Define a method in your service class
    public List<InstrumentAttribute> getOpenInstrumentAttributesByInstrumentId(String instrumentId, String attributeId,String tenantId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId.toUpperCase()));
        query.addCriteria(Criteria.where("attributeId").is(attributeId.toUpperCase()));
        query.addCriteria(Criteria.where("endDate").is(null));

        return this.dataService.fetchData(query, tenantId, InstrumentAttribute.class);
    }

    // Define a method in your service class
    public InstrumentAttribute getInstrumentAttributeByPeriodId(String tenantId, String attributeId, String instrumentId, int periodId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("attributeId").is(attributeId));
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId));
        query.addCriteria(Criteria.where("periodId").lte(periodId));

        // Sort by versionId in descending order
        query.with(Sort.by(Sort.Order.desc("versionId")));

        // Limit the result to 1
        query.limit(1);

        // Fetch the data
        List<InstrumentAttribute> result = this.dataService.fetchData(query, tenantId, InstrumentAttribute.class);

        // Return the first result if available, otherwise return null
        return result.isEmpty() ? null : result.get(0);
    }

    // Define a method in your service class
    public InstrumentAttribute getInstrumentAttributeByVersionId(String tenantId, long versionId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("versionId").is(versionId));

        // Limit the result to 1
        query.limit(1);

        // Fetch the data
        List<InstrumentAttribute> result = this.dataService.fetchData(query, tenantId, InstrumentAttribute.class);

        // Return the first result if available, otherwise return null
        return result.isEmpty() ? null : result.get(0);
    }



    public InstrumentAttribute getInstrumentAttributeByVersionId(long versionId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("versionId").is(versionId));

        // Limit the result to 1
        query.limit(1);

        // Fetch the data
        List<InstrumentAttribute> result = this.dataService.fetchData(query, InstrumentAttribute.class);

        // Return the first result if available, otherwise return null
        return result.isEmpty() ? null : result.get(0);
    }

    public List<InstrumentAttribute> findByAttributeIdAndInstrumentId(String attributeId, String instrumentId) {
        return instrumentAttributeRepository.findByAttributeIdAndInstrumentId(attributeId, instrumentId);
    }

    @Override
    public InstrumentAttribute save(InstrumentAttribute ia) {
        return this.dataService.save(ia);
    }

    public Collection<InstrumentAttribute> save(List<InstrumentAttribute> instrumentAttributes) {
        return this.dataService.saveAll(instrumentAttributes, InstrumentAttribute.class);
    }

    public Collection<InstrumentAttribute> bulkSave(Set<InstrumentAttribute> instrumentAttributes, String tenantId) {
        return this.dataService.bulkSave(instrumentAttributes,tenantId, InstrumentAttribute.class);
    }
    @Override
    public Collection<InstrumentAttribute> fetchAll() {
        return  this.dataService.fetchAllData(InstrumentAttribute.class);
    }

    @Override
    public void loadIntoCache() throws ExecutionException, InterruptedException {
        ReferenceData referenceData = this.memcachedRepository.getFromCache(this.dataService.getTenantId(), ReferenceData.class);
        if(referenceData != null) {
            int previousAccountingPeriodId = referenceData.getPrevioudAccountingPeriodId();
            this.loadIntoCache(previousAccountingPeriodId);
            int currentAccountingPeriodId = referenceData.getCurrentAccountingPeriodId();
            this.loadIntoCache(currentAccountingPeriodId);
        }
    }

    public void loadIntoCache(int accountingPeriodId) throws ExecutionException, InterruptedException {
        Set<String> instrumentIds = this.getInstrumentIdsByPeriodId(accountingPeriodId);
        String key = Key.previoudPeriodInstrumentsKey(this.dataService.getTenantId());
        boolean ifExists = this.memcachedRepository.ifExists(key);

        if(ifExists) {
            this.memcachedRepository.delete(key);
        }
        this.memcachedRepository.putInCache(key, instrumentIds);
    }
    /**
     * to get all active instruments
     * @param periodId
     * @return
     */
    public Set<String> getInstrumentIdsByPeriodId(int periodId) {
        Query query = new Query(Criteria.where("periodId").is(periodId));
        List<InstrumentAttribute> activities = this.dataService.fetchData(query, InstrumentAttribute.class);
        List<String> instrumentIds = new ArrayList<>();
        for (InstrumentAttribute ia : activities) {
            instrumentIds.add(ia.getInstrumentId());
        }
        return new HashSet<>(instrumentIds);
    }

    public void addIntoCache(String tenantId, InstrumentAttribute instrumentAttribute) {
        String key = Key.instrumentAttributeList(tenantId);
        CacheMap<InstrumentAttribute> instrumentAttributeCacheMap;
        if(this.memcachedRepository.ifExists(key)) {
            instrumentAttributeCacheMap = this.memcachedRepository.getFromCache(key, CacheMap.class);
          }else{
            instrumentAttributeCacheMap = new CacheMap<InstrumentAttribute>();
         }
        String iaKey = this.getKey(tenantId
                , instrumentAttribute);

        instrumentAttributeCacheMap.put(iaKey,instrumentAttribute);
        this.memcachedRepository.putInCache(key, instrumentAttributeCacheMap);
    }

    private String getKey(String tenantId, InstrumentAttribute instrumentAttribute) {
        return this.getKey(tenantId
                , instrumentAttribute.getAttributeId()
                , instrumentAttribute.getInstrumentId()
                , instrumentAttribute.getPeriodId());
    }

    private String getKey(String tenantId, String attributeId, String instrumentId, int periodId) {
        return Key.instrumentAttributeKey(tenantId
                , attributeId
                , instrumentId
                , periodId);
    }
    public void getInstrumentAttribute(String tenantId, String attributeId, String instrumentId, int periodId) {
        String key = Key.instrumentAttributeList(tenantId);
        CacheMap<InstrumentAttribute> instrumentAttributeCacheMap;
        if(this.memcachedRepository.ifExists(key)) {
            instrumentAttributeCacheMap = this.memcachedRepository.getFromCache(key, CacheMap.class);
        }else{
            instrumentAttributeCacheMap = new CacheMap<InstrumentAttribute>();
        }
        String iaKey = this.getKey(tenantId
                , attributeId, instrumentId, periodId);
         InstrumentAttribute instrumentAttribute =  instrumentAttributeCacheMap.getValue(iaKey);
         if(instrumentAttribute == null) {
             instrumentAttribute = this.getInstrumentAttributeByPeriodId(tenantId, attributeId, instrumentId, periodId);
             if(instrumentAttribute != null) {
                 this.addIntoCache(tenantId, instrumentAttribute);
             }
         }
    }

    public InstrumentAttribute createInstrumentAttribute(String instrumentId,
                                                         String attributeId,
                                                         Date effectiveDate,
                                                         int periodId,
                                                         int postingDate,
                                                         Source source,
                                                         Map<String, Object> attributes) {
        return instrumentAttributeFactory.create(
                instrumentId,
                attributeId,
                effectiveDate, // effectiveDate
                periodId, // periodId
                postingDate,
                source,
                new HashMap<>() // attributes
        );
    }

    public List<InstrumentAttribute> findRecordsWhereEndDateIsNull(String instrumentId, String attributeId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("endDate").is(null)
                .and("instrumentId").is(instrumentId)
                .and("attributeId").is(attributeId));
        return this.dataService.fetchData(query, InstrumentAttribute.class);
    }

    public List<InstrumentAttribute> findRecordFilterByVersionId(long versionId, String instrumentId, String attributeId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("versionId").is(versionId)
                .and("instrumentId").is(instrumentId)
                .and("attributeId").is(attributeId));
        return this.dataService.fetchData(query, InstrumentAttribute.class);
    }

    public List<InstrumentAttribute> getInstruments(Date endDate, int pageNumber, int chunkSize) {
        Query query = new Query();
        query.addCriteria(Criteria.where("endDate").is(endDate));
        query.skip(pageNumber * chunkSize);  // Skip the already processed data
        query.limit(chunkSize);  // Limit to chunk size

        // Fetch the chunk of data
        return  this.dataService.fetchData(query, InstrumentAttribute.class);
    }

    public List<String> getInstrumentIds(Date endDate, int pageNumber, int chunkSize) {
        List<InstrumentAttribute> instruments = getInstruments(endDate, pageNumber, chunkSize);
        List<String> instrumentIds = new ArrayList<>();
        for (InstrumentAttribute instrument : instruments) {
            instrumentIds.add(instrument.getInstrumentId());
        }

        return instrumentIds;
    }

    public Set<String> getColumnNames() {

        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate();
        Set<String> columns = new HashSet<>();

        // Get one document to extract the field names (columns)
        Map<String, Object> doc = mongoTemplate.findOne(Query.query(Criteria.where("_id").exists(true)), Map.class, "InstrumentAttribute");

        if (doc != null) {
            return doc.keySet();
        }

        return null;
    }

    public boolean existsReplay(String instrumentId, String attributeId, Integer effectiveDate) {
        // Validate input parameters
        if (instrumentId == null || attributeId == null || effectiveDate == null) {
            throw new IllegalArgumentException(String.format(
                    "Invalid parameters[no parameter should be a null]: instrumentId=%s, attributeId=%s, effectiveDate=%d",
                    instrumentId, attributeId, effectiveDate));
        }


        // Create the query
        Query query = new Query();
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId));
        query.addCriteria(Criteria.where("attributeId").is(attributeId));
        query.addCriteria(Criteria.where("maxTransactionDate").gt(effectiveDate));

        // Get the MongoTemplate instance
        MongoTemplate template = this.dataService.getMongoTemplate();

        // Check if the record exists
        return template.exists(query, InstrumentActivityState.class);
    }

    public List<InstrumentAttribute> getDistinctInstrumentsByInstrumentId(Date endDate, int pageNumber, int chunkSize) {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("endDate").is(endDate)),
                Aggregation.sort(Sort.by(Sort.Direction.ASC, "instrumentId")), // Optional sort
                Aggregation.group("instrumentId")
                        .first(Aggregation.ROOT).as("instrumentAttribute"), // Take full document
                Aggregation.replaceRoot("instrumentAttribute"), // Restore original structure
                Aggregation.skip((long) pageNumber * chunkSize),
                Aggregation.limit(chunkSize)
        );

        // Get the MongoTemplate instance
        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate();

        AggregationResults<InstrumentAttribute> results =
                mongoTemplate.aggregate(aggregation, "InstrumentAttribute", InstrumentAttribute.class);

        return results.getMappedResults();
    }

    public Integer getLatestActivityPostingDate(String tenantId) {
        // Execute the query to get the result
        InstrumentAttribute result = this.get(tenantId, "postingDate");

        // Return the periodId or 0 if no result is found
        return result != null ? result.getPostingDate() : 0;
    }

    public InstrumentAttribute get(String tenantId, String ... properties) {
        // Fetch the MongoTemplate for the specified tenant
        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate(tenantId);

        // Create a query to find the maximum periodId
        Query query = new Query();
        query.with(Sort.by(Sort.Direction.DESC, properties)); // Sort by periodId in descending order
        query.limit(1); // Limit the result to only one document

        // Execute the query to get the result
        return  mongoTemplate.findOne(query, InstrumentAttribute.class);
    }

}
