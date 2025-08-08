package com.fyntrac.reporting.controller;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.GeneralLedgerEnteryStage;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.reporting.service.JournalEntryReportingService;
import com.fyntrac.reporting.service.TransactionActivityReportingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/reporting/transaction-activity")
@Slf4j
public class TransactionActivityReportController {
    private final TransactionActivityReportingService reportingService;
    public TransactionActivityReportController(TransactionActivityReportingService transactionActivityReportingService) {
        this.reportingService = transactionActivityReportingService;
    }

    @GetMapping("/get/attributes")
    public ResponseEntity<List<Records.DocumentAttribute>> getReportAttribute() {
        try {
            List<Records.DocumentAttribute> reportAttributes = this.reportingService.getReportAttributes("TransactionActivity");
            return new ResponseEntity<>(reportAttributes, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/execute")
    public ResponseEntity<List<TransactionActivity>> executeJournalEntryReport(@RequestBody List<Records.QueryCriteriaItem> queryCriteria) {
        try {
            List<TransactionActivity> reportData = this.reportingService.executeReport(queryCriteria);
            return new ResponseEntity<>(reportData, HttpStatus.OK);
        }catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}

