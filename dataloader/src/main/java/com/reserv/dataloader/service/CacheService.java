package com.reserv.dataloader.service;

import com.fyntrac.common.service.aggregation.AttributeLevelAggregationService;
import com.fyntrac.common.service.aggregation.InstrumentLevelAggregationService;
import com.fyntrac.common.service.aggregation.MetricLevelAggregationService;
import org.springframework.beans.factory.annotation.Autowired;
import com.fyntrac.common.service.InstrumentAttributeService;
import org.springframework.stereotype.Service;
import com.fyntrac.common.service.AccountingPeriodDataUploadService;
import java.util.concurrent.ExecutionException;

@Service
public class CacheService {

    AccountingPeriodDataUploadService accountingPeriodService;
    InstrumentAttributeService instrumentAttributeService;
    AttributeLevelAggregationService attributeLevelAggregationService;
    InstrumentLevelAggregationService instrumentLevelAggregationService;
    MetricLevelAggregationService metricLevelAggregationService;

    @Autowired
    CacheService(AccountingPeriodDataUploadService accountingPeriodService
    , InstrumentAttributeService instrumentAttributeService
    , AttributeLevelAggregationService attributeLevelAggregationService
    , InstrumentLevelAggregationService instrumentLevelAggregationService
    ,MetricLevelAggregationService metricLevelAggregationService) {
        this.accountingPeriodService = accountingPeriodService;
        this.attributeLevelAggregationService = attributeLevelAggregationService;
        this.instrumentAttributeService = instrumentAttributeService;
        this.instrumentLevelAggregationService = instrumentLevelAggregationService;
        this.metricLevelAggregationService = metricLevelAggregationService;

    }

    public void loadIntoCache() throws ExecutionException, InterruptedException {
        this.accountingPeriodService.loadIntoCache();
        // this.instrumentAttributeService.loadIntoCache();
        this.attributeLevelAggregationService.loadIntoCache();
        this.instrumentLevelAggregationService.loadIntoCache();
        // this.metricLevelAggregationService.loadIntoCache();
    }

    public void purgeCache(String key) throws ExecutionException, InterruptedException {
        this.accountingPeriodService.getMemcachedRepository().delete(key);
    }
}
