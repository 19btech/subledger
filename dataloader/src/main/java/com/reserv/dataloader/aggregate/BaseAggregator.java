package com.reserv.dataloader.aggregate;

import com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.fyntrac.common.utils.Key;
import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.service.SettingsService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Slf4j
public abstract class BaseAggregator implements Aggregator {
    protected final MemcachedRepository memcachedRepository;
    protected  final DataService<?> dataService;
    protected final ReferenceData referenceData;
    protected  final SettingsService settingsService;
    protected final String tenantId;
    protected List<String> ltdObjectCleanupList = null;
    @Getter
    protected final AccountingPeriodService accountingPeriodService;
    protected final AggregationService aggregationService;
    protected final AggregationRequest aggregationRequest;

    public BaseAggregator(MemcachedRepository memcachedRepository
            ,DataService<?> dataService
            , SettingsService settingsService
                          , AccountingPeriodService accountingPeriodService
                          , AggregationService aggregationService
                          , AggregationRequest aggregationRequest
            , String tenantId) {
        this.memcachedRepository = memcachedRepository;
        this.dataService = dataService;
        this.settingsService = settingsService;
        this.tenantId = tenantId;
        TenantContextHolder.setTenant(tenantId);
        this.dataService.setTenantId(tenantId);
        this.referenceData = this.memcachedRepository.getFromCache(this.tenantId, ReferenceData.class);
        this.dataService.setTenantId(tenantId);
        ltdObjectCleanupList = new ArrayList<>(0);
        this.accountingPeriodService = accountingPeriodService;
        this.aggregationService = aggregationService;
        this.aggregationRequest = aggregationRequest;
    }

    public void aggregate(List<String> activities) {
        for(String transactionActivityKey : activities) {
            TransactionActivity transactionActivity = this.memcachedRepository.getFromCache(transactionActivityKey, TransactionActivity.class);
            this.aggregate(transactionActivity);
        }
    }

    protected List<String> getMetrics(TransactionActivity activity) {
        List<String> metricsList = new ArrayList<>();

        try {
            String metricKey = Key.allMetricList(tenantId);
            CacheMap<Set<String>> metrics = this.memcachedRepository.getFromCache(metricKey, CacheMap.class);

            if (metrics != null) {
                Set<String> metricSet = metrics.getValue(activity.getTransactionName().toUpperCase());
                if (metricSet != null) {
                    metricsList = new ArrayList<>(metricSet);
                } else {
                    log.warn("No metrics found for transaction name: {}", activity.getTransactionName());
                }
            } else {
                log.warn("Metrics cache is null for key: {}", metricKey);
            }
        } catch (Exception e) {
            log.error("Error retrieving metrics for transaction activity: {}", activity, e);
        }

        return metricsList;
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
