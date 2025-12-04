package com.fyntrac.reporting.controller;


import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.Option;
import com.fyntrac.reporting.service.CustomOperationalDataReportingService;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/reporting/custom-operational-data")
@Slf4j
public class CustomOperationalDataReportController {
    private final CustomOperationalDataReportingService reportingService;
    public CustomOperationalDataReportController(CustomOperationalDataReportingService reportingService) {
        this.reportingService = reportingService;
    }

    @GetMapping("/get/attributes/{tableName}")
    public ResponseEntity<List<Records.DocumentAttribute>> getReportAttribute(@PathVariable String tableName) {
        try {
            List<Records.DocumentAttribute> reportAttributes = this.reportingService.getReportAttributes(tableName);
            return new ResponseEntity<>(reportAttributes, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/table-names")
    public ResponseEntity<List<Option>> getRefDataTables() {
        try {
            List<Option> reportAttributes = this.reportingService.getRefDataTables();
            return new ResponseEntity<>(reportAttributes, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }


    @PostMapping("/execute/{id}")
    public ResponseEntity<List<Document>> executeJournalEntryReport(
            @PathVariable String id,
            @RequestBody List<Records.QueryCriteriaItem> queryCriteria) {

        try {
            List<Document> reportData =
                    reportingService.executeReport(id, queryCriteria);

            return ResponseEntity.ok(reportData);

        } catch (Exception e) {
            log.error("Error executing journal report for id {}: {}", id, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

}
