package com.reserv.dataloader.service.aggregation;

import com.fyntrac.common.entity.Aggregation;
import com.fyntrac.common.repository.MemcachedRepository;
import com.reserv.dataloader.service.CacheBasedService;
import com.fyntrac.common.service.DataService;
import com.reserv.dataloader.service.SettingsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
        for(Aggregation aggregation : this.fetchAll()) {
            String key = this.dataService.getTenantId() + aggregation.hashCode();
            this.loadIntoCache(aggregation);
        }
    }

    private void loadIntoCache(Aggregation aggregation) {
        String key = this.dataService.getTenantId() + aggregation.hashCode();
        List<String> metricList = null;
        if(this.memcachedRepository.ifExists(key)) {
            metricList = this.memcachedRepository.getFromCache(key, List.class);
        }else{
            metricList = new ArrayList<>(0);
        }
        if(!metricList.contains(aggregation.getMetricName())) {
            metricList.add(aggregation.getMetricName());
        }

        this.memcachedRepository.putInCache(key, metricList);
    }
}
