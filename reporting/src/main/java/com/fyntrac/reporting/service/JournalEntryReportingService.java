package com.fyntrac.reporting.service;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.GeneralLedgerEntery;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.utils.MongoQueryGenerator;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class JournalEntryReportingService {
    private final DataService<GeneralLedgerEntery> dataService;

    public JournalEntryReportingService(DataService<GeneralLedgerEntery> dataService) {
        this.dataService = dataService;
    }

    public List<GeneralLedgerEntery> executeReport(List<Records.QueryCriteriaItem> queryCriteria) {

        Query reportQuery = MongoQueryGenerator.generateQuery(queryCriteria);
        // Log the query as a string
        Document queryDocument = reportQuery.getQueryObject();
        log.info("JournalEntryReporting Query: {}", queryDocument.toJson());

        return this.dataService.fetchData(reportQuery, GeneralLedgerEntery.class);
    }

    public List<Records.DocumentAttribute> getReportAttributes(String documentName) {
        return this.dataService.getAttributesWithTypes(documentName);

    }
}
