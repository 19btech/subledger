package com.fyntrac.common.service.aggregation;

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

    public void aggregate(Collection<TransactionActivity> activityCollection, int previousPostingDate) {

        this.attributeLevelAggregationService.aggregate(activityCollection, previousPostingDate);
        this.instrumentLevelAggregationService.aggregate(activityCollection, previousPostingDate);
        this.metricLevelAggregationService.aggregate(activityCollection, previousPostingDate);

    }
}
