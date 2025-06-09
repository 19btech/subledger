package com.fyntrac.common.service.aggregation;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.Aggregation;
import com.fyntrac.common.entity.AttributeLevelLtd;
import com.fyntrac.common.entity.InstrumentLevelLtd;
import com.fyntrac.common.entity.MetricLevelLtd;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.CacheBasedService;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AggregationService  extends CacheBasedService<Aggregation> {

    @Autowired
    public AggregationService(DataService<Aggregation> dataService
                              , MemcachedRepository memcachedRepository) {
       super(dataService, memcachedRepository);
    }

    @Override
    public Aggregation save(Aggregation aggregation) {
        this.dataService.save(aggregation);
        String key = this.dataService.getTenantId() + aggregation.getTransactionName();
        List<String> metricList = null;
        if(this.memcachedRepository.ifExists(key)) {
            metricList = this.memcachedRepository.getFromCache(key, List.class);
        }else{
            metricList = new ArrayList<>(0);
        }
        metricList.add(aggregation.getMetricName());
        this.memcachedRepository.putInCache(key, metricList);
        return aggregation;
    }

    @Override
    public Collection<Aggregation> fetchAll() {
        return dataService.fetchAllData(Aggregation.class);
    }

    public Collection<Records.MetricNameRecord> fetchMetricNames() {
      Collection<String> metrics = this.dataService.getMongoTemplate().query(Aggregation.class)  // Replace Metric.class with your actual class
                .distinct("metricName")          // Specify the field name
                .as(String.class)                // Specify the return type
                .all();

        // Map the distinct names to MetricRecord objects
        return metrics.stream()
                .map(Records.MetricNameRecord::new)         // Create a new MetricRecord for each distinct name
                .collect(Collectors.toList());
    }

    @Override
    public void loadIntoCache() {
        CacheMap<Set<String>> metricMap = new CacheMap<>();
        for(Aggregation aggregation : this.fetchAll()) {
            String key = this.dataService.getTenantId() + aggregation.hashCode();
            this.loadIntoCache(aggregation, metricMap);
        }

        this.memcachedRepository.putInCache(Key.allMetricList(this.dataService.getTenantId()),metricMap);
    }

    private void loadIntoCache(Aggregation aggregation, CacheMap<Set<String>> metricMap) {
        String key = aggregation.getTransactionName().toUpperCase();
        Set<String> metricList = metricMap.getValue(key);

        if(metricList == null) {
            metricList = new HashSet<>(0);
        }
        metricList.add(aggregation.getMetricName());
        metricMap.put(key, metricList);
    }

    /**
     * Fetches aggregation metrics for a given transaction.
     * <p>
     * The method first checks if the metrics exist in Memcached. If available, it returns the cached data.
     * Otherwise, it fetches data from MongoDB, updates the cache, and returns the retrieved results.
     * </p>
     *
     * @param transactionName The name of the transaction for which metrics are fetched.
     * @return A list of {@link Aggregation} objects containing the metrics. If no records are found, an empty list is returned.
     */
    public List<Aggregation> getMetrics(String transactionName) {
        String key = getKey(transactionName);
        log.info("Fetching metrics for transaction: {}", transactionName);

        try {
            // Check if data exists in Memcached
            if (memcachedRepository.ifExists(key)) {
                log.debug("Cache hit for key: {}", key);
                CacheList<Aggregation> metrics = memcachedRepository.getFromCache(key, CacheList.class);
                return metrics.getList();
            } else {
                log.debug("Cache miss for key: {}. Fetching from MongoDB...", key);
            }
        } catch (Exception e) {
            log.error("Error accessing Memcached for key: {}. Proceeding with MongoDB fetch.", key, e);
        }

        // Fetch from MongoDB
        Query query = new Query(Criteria.where("transactionName").is(transactionName));
        List<Aggregation> metricList;
        try {
            metricList = dataService.fetchData(query, Aggregation.class);
            log.info("Fetched {} records from MongoDB for transaction: {}", metricList.size(), transactionName);
        } catch (DataAccessException e) {
            log.error("MongoDB query failed for transaction: {}", transactionName, e);
            return Collections.emptyList(); // Return empty list to avoid null issues
        }

        // Cache fetched results
        try {
            CacheList<Aggregation> metrics = new CacheList<>();
            metrics.addAll(metricList);
            memcachedRepository.putInCache(key, metrics, 3600);
            log.debug("Cached {} records for key: {}", metricList.size(), key);
        } catch (Exception e) {
            log.error("Failed to cache data for key: {}", key, e);
        }

        return metricList;
    }

    /**
     * Retrieves unique combinations of metricName, instrumentId, and attributeId
     * from the attributeLevelLtd collection for postingDate >= fromPostingDate.
     * Only combinations that appear exactly once are returned.
     *
     * @param fromPostingDate the lower bound for postingDate filter (inclusive)
     * @return List of Documents each containing metricName, instrumentId, attributeId
     */
    public List<AttributeLevelLtd> getCarryOverAttributeLevelBalances(int fromPostingDate) {
        MatchOperation match = org.springframework.data.mongodb.core.aggregation.Aggregation.match(
                Criteria.where("postingDate").gte(fromPostingDate)
        );
        GroupOperation group = org.springframework.data.mongodb.core.aggregation.Aggregation.group("metricName", "instrumentId", "attributeId")
                .count().as("count");
        MatchOperation onlyUnique = org.springframework.data.mongodb.core.aggregation.Aggregation.match(Criteria.where("count").is(1));
        ProjectionOperation project = org.springframework.data.mongodb.core.aggregation.Aggregation.project()
                .and("_id.metricName").as("metricName")
                .and("_id.instrumentId").as("instrumentId")
                .and("_id.attributeId").as("attributeId");
        org.springframework.data.mongodb.core.aggregation.Aggregation agg = org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation(match, group, onlyUnique, project);
        AggregationResults<AttributeLevelLtd> results = this.dataService.getMongoTemplate().aggregate(agg, "AttributeLevelLtd", AttributeLevelLtd.class);
        return results.getMappedResults();
    }

    /**
     * Retrieves unique combinations of metricName, instrumentId, and attributeId
     * from the attributeLevelLtd collection for postingDate >= fromPostingDate.
     * Only combinations that appear exactly once are returned.
     *
     * @param fromPostingDate the lower bound for postingDate filter (inclusive)
     * @return List of Documents each containing metricName, instrumentId, attributeId
     */
    public List<InstrumentLevelLtd> getCarryOverInstrumentLevelBalances(int fromPostingDate) {
        MatchOperation match = org.springframework.data.mongodb.core.aggregation.Aggregation.match(
                Criteria.where("postingDate").gte(fromPostingDate)
        );
        GroupOperation group = org.springframework.data.mongodb.core.aggregation.Aggregation.group("metricName", "instrumentId")
                .count().as("count");
        MatchOperation onlyUnique = org.springframework.data.mongodb.core.aggregation.Aggregation.match(Criteria.where("count").is(1));
        ProjectionOperation project = org.springframework.data.mongodb.core.aggregation.Aggregation.project()
                .and("_id.metricName").as("metricName")
                .and("_id.instrumentId").as("instrumentId");

        org.springframework.data.mongodb.core.aggregation.Aggregation agg = org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation(match, group, onlyUnique, project);
        AggregationResults<InstrumentLevelLtd> results = this.dataService.getMongoTemplate().aggregate(agg, "InstrumentLevelLtd", InstrumentLevelLtd.class);
        return results.getMappedResults();
    }

    /**
     * Retrieves unique combinations of metricName, instrumentId, and attributeId
     * from the attributeLevelLtd collection for postingDate >= fromPostingDate.
     * Only combinations that appear exactly once are returned.
     *
     * @param fromPostingDate the lower bound for postingDate filter (inclusive)
     * @return List of Documents each containing metricName, instrumentId, attributeId
     */
    public List<MetricLevelLtd> getCarryOverMetricLevelBalances(int fromPostingDate) {
        MatchOperation match = org.springframework.data.mongodb.core.aggregation.Aggregation.match(
                Criteria.where("postingDate").gte(fromPostingDate)
        );
        GroupOperation group = org.springframework.data.mongodb.core.aggregation.Aggregation.group("metricName")
                .count().as("count");
        MatchOperation onlyUnique = org.springframework.data.mongodb.core.aggregation.Aggregation.match(Criteria.where("count").is(1));
        ProjectionOperation project = org.springframework.data.mongodb.core.aggregation.Aggregation.project()
                .and("_id.metricName").as("metricName");
        org.springframework.data.mongodb.core.aggregation.Aggregation agg = org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation(match, group, onlyUnique, project);
        AggregationResults<MetricLevelLtd> results = this.dataService.getMongoTemplate().aggregate(agg, "MetricLevelLtd", MetricLevelLtd.class);
        return results.getMappedResults();
    }

    /**
     * Generates a cache key based on the transaction name.
     *
     * @param transactionName The transaction name.
     * @return A unique cache key for the transaction.
     */
    private String getKey(String transactionName) {
        // return this.dataService.getTenantId()  + transactionName + "METRICS";
        return String.format("%s-%s-%s", this.dataService.getTenantId(), transactionName, "METRICS");

    }
}