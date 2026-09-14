package com.fyntrac.common.service;

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
import net.spy.memcached.internal.OperationFuture;
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

    /**
     * Bulk form of {@link #getOpenInstrumentAttributes(String, String, String)}: fetches the open
     * (endDate == null) attributes for many instruments in ONE query and returns them grouped by
     * "attributeId_instrumentId" — the same key InstrumentAttributeWriter.setEndDate() groups the
     * incoming chunk by.
     *
     * The writer used to call the single-pair method once per group, which is one Mongo round trip
     * per (attributeId, instrumentId) in the chunk — a few thousand sequential queries for a large
     * upload file. Nothing is written to Mongo between those calls (setEndDate only mutates objects
     * in memory; the chunk is persisted afterwards by the delegate writer), so prefetching once per
     * chunk sees exactly the same state as querying per group.
     *
     * The query filters on instrumentId, which is the prefix of the
     * {instrumentId, attributeId, versionId} compound index, so the $in is index-served.
     */
    public Map<String, List<InstrumentAttribute>> getOpenInstrumentAttributes(Collection<String> instrumentIds,
                                                                             String tenantId) {
        if (instrumentIds == null || instrumentIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Query query = new Query();
        query.addCriteria(Criteria.where("instrumentId").in(instrumentIds));
        query.addCriteria(Criteria.where("endDate").is(null));

        List<InstrumentAttribute> open = this.dataService.fetchData(query, tenantId, InstrumentAttribute.class);
        if (open == null || open.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, List<InstrumentAttribute>> byKey = new HashMap<>();
        for (InstrumentAttribute attribute : open) {
            byKey.computeIfAbsent(attribute.getAttributeId() + "_" + attribute.getInstrumentId(),
                    k -> new ArrayList<>()).add(attribute);
        }
        return byKey;
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

    /**
     * Caches a single InstrumentAttribute under its own key.
     *
     * This used to read-modify-write ONE tenant-wide CacheMap holding every instrument
     * attribute, once per row, from InstrumentAttributeWriter.write(). Each call did a
     * full download + deserialize (twice over — ifExists() issues its own get and throws
     * the result away), added one entry, then re-serialized and uploaded the whole map,
     * so writing N rows moved O(N^2) bytes. A measured Hearst load pushed ~11 GB through
     * memcached to maintain ~11 MB of cache, and the aggregate entry was already at 41%
     * of memcached's 1 MB item_size_max — past which the set silently fails, ifExists()
     * then reports false, and every row rebuilds the map from empty forever.
     *
     * Storing each attribute under Key.instrumentAttributeKey (the key the inner map was
     * already using) makes this O(1) per row, needs no read at all, and removes the 1 MB
     * ceiling since each entry holds one attribute.
     */
    public void addIntoCache(String tenantId, InstrumentAttribute instrumentAttribute) {
        if (instrumentAttribute == null) {
            return;
        }
        this.memcachedRepository.putInCache(this.getKey(tenantId, instrumentAttribute), instrumentAttribute);
    }

    /**
     * Caches a whole chunk in one pass.
     *
     * spymemcached's set() is asynchronous, so the sets pipeline over the single
     * connection instead of paying a round trip each. We still block until they land
     * before returning, so a subsequent read in the same step cannot miss a write we
     * just issued.
     */
    public void addIntoCache(String tenantId, Collection<InstrumentAttribute> instrumentAttributes) {
        if (instrumentAttributes == null || instrumentAttributes.isEmpty()) {
            return;
        }
        List<OperationFuture<Boolean>> pending = new ArrayList<>(instrumentAttributes.size());
        for (InstrumentAttribute instrumentAttribute : instrumentAttributes) {
            if (instrumentAttribute == null) {
                continue;
            }
            pending.add(this.memcachedRepository.putInCache(
                    this.getKey(tenantId, instrumentAttribute), instrumentAttribute));
        }
        for (OperationFuture<Boolean> future : pending) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while caching instrument attributes", e);
            } catch (Exception e) {
                // A cache write failing is not fatal: every reader falls back to Mongo.
                log.warn("Failed to cache an instrument attribute for tenant {}: {}", tenantId, e.getMessage());
            }
        }
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
    /**
     * Warms the cache for one instrument attribute, reading it from the per-attribute key
     * written by addIntoCache() and falling back to Mongo on a miss.
     */
    public void getInstrumentAttribute(String tenantId, String attributeId, String instrumentId, int periodId) {
        String iaKey = this.getKey(tenantId, attributeId, instrumentId, periodId);
        InstrumentAttribute instrumentAttribute =
                this.memcachedRepository.getFromCache(iaKey, InstrumentAttribute.class);
        if (instrumentAttribute == null) {
            instrumentAttribute = this.getInstrumentAttributeByPeriodId(tenantId, attributeId, instrumentId, periodId);
            if (instrumentAttribute != null) {
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

    public List<InstrumentAttribute> getInstrumentAttributeByEffectiveDateGte(String instrumentId, String attributeId
            , Date effectiveDate) {
       return  this.instrumentAttributeRepository.findByInstrumentIdAndAttributeIdAndEffectiveDateGte(instrumentId,
                attributeId, effectiveDate);
    }
}
