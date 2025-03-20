package com.fyntrac.model.service;

import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.service.aggregation.AttributeLevelAggregationService;
import com.fyntrac.common.service.aggregation.InstrumentLevelAggregationService;
import com.fyntrac.common.service.aggregation.MetricLevelAggregationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collection;

@Service
@Slf4j
public class CommonAggregationService {

    private final AttributeLevelAggregationService attributeLevelAggregationService;
    private final InstrumentLevelAggregationService instrumentLevelAggregationService;
    private final MetricLevelAggregationService metricLevelAggregationService;
    public CommonAggregationService(AttributeLevelAggregationService attributeLevelAggregationService
    , InstrumentLevelAggregationService instrumentLevelAggregationService
    , MetricLevelAggregationService metricLevelAggregationService) {
        this.attributeLevelAggregationService = attributeLevelAggregationService;
        this.instrumentLevelAggregationService = instrumentLevelAggregationService;
        this.metricLevelAggregationService = metricLevelAggregationService;
    }

    public void aggregate(Collection<TransactionActivity> activityCollection) {

        this.attributeLevelAggregationService.aggregate(activityCollection);
        this.instrumentLevelAggregationService.aggregate(activityCollection);
        this.metricLevelAggregationService.aggregate(activityCollection);

    }
}
