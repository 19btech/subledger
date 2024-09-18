package com.reserv.dataloader.service;

import com.reserv.dataloader.service.aggregation.AttributeLevelAggregationService;
import com.reserv.dataloader.service.aggregation.InstrumentLevelAggregationService;
import com.reserv.dataloader.service.aggregation.MetricLevelAggregationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
public class CacheService {

    AccountingPeriodService accountingPeriodService;
    InstrumentAttributeService instrumentAttributeService;
    AttributeLevelAggregationService attributeLevelAggregationService;
    InstrumentLevelAggregationService instrumentLevelAggregationService;
    MetricLevelAggregationService metricLevelAggregationService;

    @Autowired
    CacheService(AccountingPeriodService accountingPeriodService
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

    public void loadIntoCache(){
        this.accountingPeriodService.loadIntoCache();
        // this.instrumentAttributeService.loadIntoCache();
        this.attributeLevelAggregationService.loadIntoCache();
        this.instrumentLevelAggregationService.loadIntoCache();
        this.metricLevelAggregationService.loadIntoCache();
    }
}
