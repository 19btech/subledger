package com.fyntrac.reporting.controller;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.reporting.service.AttributeRollforwardReportingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/reporting/attribute-rollforward")
@Slf4j
public class AttributeRollforwardReportController {
    private final AttributeRollforwardReportingService reportingService;
    public AttributeRollforwardReportController(AttributeRollforwardReportingService attributeRollforwardReportingService) {
        this.reportingService = attributeRollforwardReportingService;
    }

    @GetMapping("/get/attributes")
    public ResponseEntity<List<Records.DocumentAttribute>> getReportAttribute() {
        try {
            List<Records.DocumentAttribute> reportAttributes = this.reportingService.getReportAttributes("AttributeLevelLtd");
            return new ResponseEntity<>(reportAttributes, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/execute")
    public ResponseEntity<List<Records.FlatAttributeLevelLtdRecord>> executeJournalEntryReport(@RequestBody List<Records.QueryCriteriaItem> queryCriteria) {
        try {
            List<Records.FlatAttributeLevelLtdRecord> reportData = this.reportingService.executeReport(queryCriteria);
            return new ResponseEntity<>(reportData, HttpStatus.OK);
        }catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}

