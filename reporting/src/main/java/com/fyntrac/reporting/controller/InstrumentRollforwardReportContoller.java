package com.fyntrac.reporting.controller;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.InstrumentLevelLtd;
import com.fyntrac.reporting.service.InstrumentRollforwardReportingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/reporting/instrument-rollforward")
@Slf4j
public class InstrumentRollforwardReportContoller {
    private final InstrumentRollforwardReportingService reportingService;
    public InstrumentRollforwardReportContoller(InstrumentRollforwardReportingService instrumentRollforwardReportingService) {
        this.reportingService = instrumentRollforwardReportingService;
    }

    @GetMapping("/get/attributes")
    public ResponseEntity<List<Records.DocumentAttribute>> getReportAttribute() {
        try {
            List<Records.DocumentAttribute> reportAttributes = this.reportingService.getReportAttributes("InstrumentLevelLtd");
            return new ResponseEntity<>(reportAttributes, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/execute")
    public ResponseEntity<List<Records.FlatInstrumentLevelLtdRecord>> executeJournalEntryReport(@RequestBody List<Records.QueryCriteriaItem> queryCriteria) {
        try {
            List<Records.FlatInstrumentLevelLtdRecord> reportData = this.reportingService.executeReport(queryCriteria);
            return new ResponseEntity<>(reportData, HttpStatus.OK);
        }catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
