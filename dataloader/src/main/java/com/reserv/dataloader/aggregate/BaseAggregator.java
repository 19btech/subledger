package com.reserv.dataloader.aggregate;

import  com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.DataService;
import com.reserv.dataloader.service.SettingsService;
import com.fyntrac.common.utils.Key;
import com.fyntrac.common.cache.collection.CacheMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public abstract class BaseAggregator implements Aggregator {
    protected final MemcachedRepository memcachedRepository;
    protected  final DataService<?> dataService;
    protected final ReferenceData referenceData;
    protected  final SettingsService settingsService;
    protected final String tenantId;
    protected List<String> ltdObjectCleanupList = null;
    public BaseAggregator(MemcachedRepository memcachedRepository
            ,DataService<?> dataService
            , SettingsService settingsService
            , String tenantId) {
        this.memcachedRepository = memcachedRepository;
        this.dataService = dataService;
        this.settingsService = settingsService;
        this.tenantId = tenantId;
        this.referenceData = this.memcachedRepository.getFromCache(this.tenantId, ReferenceData.class);
        this.dataService.setTenantId(tenantId);
        ltdObjectCleanupList = new ArrayList<>(0);
    }

    public void aggregate(List<String> activities) {
        for(String transactionActivityKey : activities) {
            TransactionActivity transactionActivity = this.memcachedRepository.getFromCache(transactionActivityKey, TransactionActivity.class);
            this.aggregate(transactionActivity);
        }
    }

    protected List<String> getMetrics(TransactionActivity activity) {
        String metricKey = Key.allMetricList(tenantId);
        CacheMap<Set<String>> metrics = this.memcachedRepository.getFromCache(metricKey, CacheMap.class);
        Set<String> metricSet = metrics.getValue(activity.getTransactionName().toUpperCase());
        return new ArrayList<>(metricSet);
    }


    abstract public void aggregate(TransactionActivity activity);

    public void cleanup() throws ExecutionException, InterruptedException {
        for(String objKey : ltdObjectCleanupList) {
            this.memcachedRepository.delete(objKey);
        }
    }

    public List<String> getCleanupList(){
        return ltdObjectCleanupList;
    }
}
