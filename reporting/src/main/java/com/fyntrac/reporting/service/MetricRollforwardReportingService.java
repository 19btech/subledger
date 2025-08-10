package com.fyntrac.reporting.service;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.MetricLevelLtd;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.utils.MongoQueryGenerator;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class MetricRollforwardReportingService {
    private final DataService<MetricLevelLtd> dataService;

    public MetricRollforwardReportingService(DataService<MetricLevelLtd> dataService) {
        this.dataService = dataService;
    }

    public List<Records.FlatMetricLevelLtdRecord> executeReport(List<Records.QueryCriteriaItem> queryCriteria) {

        Query reportQuery = MongoQueryGenerator.generateQuery(queryCriteria);
        reportQuery.fields()
                .include("_id")
                .include("metricName")
                .include("instrumentId")
                .include("attributeId")
                .include("accountingPeriodId")
                .include("postingDate")
                .include("balance.activity")
                .include("balance.beginningBalance")
                .include("balance.endingBalance");

        // Log the query as a string
        Document queryDocument = reportQuery.getQueryObject();
        log.info("Attribute Rollforward Query: {}", queryDocument.toJson());

        return this.dataService
                .getMongoTemplate()
                .find(reportQuery, Records.FlatMetricLevelLtdRecord.class, "MetricLevelLtd");
    }

    public List<Records.DocumentAttribute> getReportAttributes(String documentName) {
        return this.dataService.getAttributesWithTypes(documentName);

    }
}