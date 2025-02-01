package com.fyntrac.common.service.aggregation;

import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.Aggregation;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.CacheBasedService;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.utils.Key;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class AggregationService  extends CacheBasedService<Aggregation> {


    @Autowired
    public AggregationService(DataService<Aggregation> dataService
                              , MemcachedRepository memcachedRepository) {
       super(dataService, memcachedRepository);
    }

    @Override
    public void save(Aggregation aggregation) {
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
}
