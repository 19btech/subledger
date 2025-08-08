package com.fyntrac.reporting.controller;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.GeneralLedgerEnteryStage;
import com.fyntrac.reporting.service.JournalEntryReportingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/reporting/jeReport")
@Slf4j
public class JournalEntryReportController {
    private final JournalEntryReportingService journalEntryReportingService;
    public JournalEntryReportController(JournalEntryReportingService journalEntryReportingService) {
        this.journalEntryReportingService = journalEntryReportingService;
    }

    @GetMapping("/get/attributes")
    public ResponseEntity<List<Records.DocumentAttribute>> getReportAttribute() {
        try {
            List<Records.DocumentAttribute> reportAttributes = this.journalEntryReportingService.getReportAttributes("GeneralLedgerEnteryStage");
            return new ResponseEntity<>(reportAttributes, HttpStatus.OK);
        } catch (Exception e) {
        // Log the exception for debugging purposes
        log.error(e.getLocalizedMessage());
        return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    }
    }

    @PostMapping("/execute")
    public ResponseEntity<List<GeneralLedgerEnteryStage>> executeJournalEntryReport(@RequestBody List<Records.QueryCriteriaItem> queryCriteria) {
        try {
            List<GeneralLedgerEnteryStage> reportData = this.journalEntryReportingService.executeReport(queryCriteria);
            return new ResponseEntity<>(reportData, HttpStatus.OK);
        }catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
