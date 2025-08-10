package com.fyntrac.reporting.service;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.InstrumentLevelLtd;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.utils.MongoQueryGenerator;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class InstrumentRollforwardReportingService {
    private final DataService<InstrumentLevelLtd> dataService;

    public InstrumentRollforwardReportingService(DataService<InstrumentLevelLtd> dataService) {
        this.dataService = dataService;
    }

    public List<Records.FlatInstrumentLevelLtdRecord> executeReport(List<Records.QueryCriteriaItem> queryCriteria) {

        Query reportQuery = MongoQueryGenerator.generateQuery(queryCriteria);
        reportQuery.fields()
                .include("_id")
                .include("metricName")
                .include("instrumentId")
                .include("accountingPeriodId")
                .include("postingDate")
                .include("balance.activity")
                .include("balance.beginningBalance")
                .include("balance.endingBalance");

        // Log the query as a string
        Document queryDocument = reportQuery.getQueryObject();
        log.info("Instrument Rollforward Query: {}", queryDocument.toJson());
        return this.dataService
                .getMongoTemplate()
                .find(reportQuery, Records.FlatInstrumentLevelLtdRecord.class, "InstrumentLevelLtd");

    }

    public List<Records.DocumentAttribute> getReportAttributes(String documentName) {
        return this.dataService.getAttributesWithTypes(documentName);

    }
}
