package com.reserv.dataloader.service.aggregation;

import com.fyntrac.common.entity.Aggregation;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.CacheBasedService;
import com.fyntrac.common.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.utils.Key;
import java.util.*;

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
