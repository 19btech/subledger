package com.fyntrac.reporting.controller;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.MetricLevelLtd;
import com.fyntrac.reporting.service.DashboardService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@RestController
@RequestMapping("/api/reporting/dashboard")
@Slf4j
public class DashboardController {

    private final DashboardService dashboardService;
    public DashboardController(DashboardService dashboardService) {
        this.dashboardService = dashboardService;
    }

    @GetMapping("/get/widget-data")
    public ResponseEntity<List<MetricLevelLtd>> getReportAttribute() {
        try {
            List<MetricLevelLtd> widgetData = dashboardService.getWidgetData();
            return new ResponseEntity<>(widgetData, HttpStatus.OK);
        }catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/trend-analysis-data")
    public ResponseEntity<Records.TrendAnalysisRecord> getTrendAnalysisDate() {
        try {
            Records.TrendAnalysisRecord trendAnalysisRecord = dashboardService.getTrendAnalysisData();
            return new ResponseEntity<>(trendAnalysisRecord, HttpStatus.OK);
        }catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/ranked-metrics")
    public ResponseEntity<List<Records.RankedMetricRecord>> getRankedMetrics() {
        try {
            List<Records.RankedMetricRecord> rankedMetricRecordList = dashboardService.getTop5MetricsByMaxEndingBalance();
            return new ResponseEntity<>(rankedMetricRecordList, HttpStatus.OK);
        }catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/mom-activity-data")
    public ResponseEntity<Records.MonthOverMonthMetricActivityRecord> getMonthOverMonthActivities() {
        try {
            Records.MonthOverMonthMetricActivityRecord monthOverMonthActivityData = dashboardService.getMonthOverMonthActivityData();
            return new ResponseEntity<>(monthOverMonthActivityData, HttpStatus.OK);
        }catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

}
