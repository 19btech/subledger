package com.reserv.dataloader.aggregate;

import com.reserv.dataloader.config.ReferenceData;
import com.reserv.dataloader.entity.*;
import com.reserv.dataloader.key.AggregationLtdKey;
import com.reserv.dataloader.key.AttributeLevelLtdKey;
import com.reserv.dataloader.repository.MemcachedRepository;
import com.reserv.dataloader.service.DataService;
import com.reserv.dataloader.service.SettingsService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        Aggregation aggregation = Aggregation.builder().transactionName(activity.getTransactionName()).build();
        String key = this.tenantId + aggregation.hashCode();
        List<String> metrics = this.memcachedRepository.getFromCache(key, List.class);
        return metrics;
    }

    abstract public void aggregate(TransactionActivity activity);

    public void cleanup() {
        for(String objKey : ltdObjectCleanupList) {
            this.memcachedRepository.delete(objKey);
        }
    }

    public List<String> getCleanupList(){
        return ltdObjectCleanupList;
    }
}
