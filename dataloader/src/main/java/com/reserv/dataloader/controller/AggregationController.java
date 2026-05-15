package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.Aggregation;
import com.fyntrac.common.entity.Option;
import com.fyntrac.common.service.aggregation.AggregationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.fyntrac.common.dto.record.Records;

import java.util.Collection;

@RestController
@RequestMapping("/api/dataloader/aggregation")
@Slf4j
public class AggregationController {
    private final AggregationService aggregationService;
    private final com.fyntrac.common.service.TransactionService transactionService;
    private final com.reserv.dataloader.validation.AggregationValidator aggregationValidator;

    @Autowired
    public AggregationController(
            AggregationService aggregationService, 
            com.fyntrac.common.service.TransactionService transactionService,
            com.reserv.dataloader.validation.AggregationValidator aggregationValidator) {
        this.aggregationService = aggregationService;
        this.transactionService = transactionService;
        this.aggregationValidator = aggregationValidator;
    }


    @PostMapping("/add")
    public ResponseEntity<?> saveDate(@RequestBody Aggregation t) {
        if (t == null) {
            return ResponseEntity.badRequest().body("Request body is required.");
        }

        // 1. Build preloaded sets for multi-tenant safe, fast REST existence validation
        java.util.Set<String> validTxNames = new java.util.HashSet<>();
        if (t.getTransactionName() != null && transactionService != null) {
            String txName = t.getTransactionName().trim();
            if (transactionService.getTransaction(txName) != null) {
                validTxNames.add(txName.toUpperCase());
            }
        }

        // 2. Check composite duplicates against DB, excluding own record (Updation logic)
        java.util.Set<String> existingKeys = new java.util.HashSet<>();
        if (t.getTransactionName() != null && t.getMetricName() != null && aggregationService != null) {
            String txName = t.getTransactionName().trim();
            String metricName = t.getMetricName().trim();
            
            java.util.List<Aggregation> existingList = aggregationService.getMetrics(txName);
            if (existingList != null) {
                for (Aggregation existing : existingList) {
                    if (existing.getMetricName() != null && existing.getMetricName().equalsIgnoreCase(metricName)) {
                        // Pick object from validation only where ID is NOT equal to request object
                        if (t.getId() == null || !existing.getId().equals(t.getId())) {
                            // Match the composite key format expected by AggregationValidator: METRIC:TRANSACTION
                            String key = metricName.toUpperCase() + ":" + txName.toUpperCase();
                            existingKeys.add(key);
                            break;
                        }
                    }
                }
            }
        }

        // Invoke stateless, composite-key validation overload
        java.util.List<com.reserv.dataloader.batch.exception.ItemValidationException.ValidationError> errors = 
                aggregationValidator.validate(t, validTxNames, existingKeys);
        
        boolean hasError = errors.stream().anyMatch(log -> "ERROR".equals(log.getSeverity()));

        if (hasError) {
            return ResponseEntity.badRequest().body(errors);
        }

        this.aggregationService.save(t);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/get/all")
    public ResponseEntity<Collection<Aggregation>> getAllAggregates() {
        try {
            Records.TrendAnalysisRecord text;
            Collection<Aggregation> transactions = this.aggregationService.fetchAll();
            return new ResponseEntity<>(transactions, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/all/options")
    public ResponseEntity<Collection<Option>> getMetricOptions() {
        try {
            Records.TrendAnalysisRecord text;
            Collection<Option> attributes = this.aggregationService.fetchMetricOptionList();
            return new ResponseEntity<>(attributes, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/metrics")
    public ResponseEntity<Collection<Records.MetricNameRecord>> getAllMetrics() {
        try {
            Collection<Records.MetricNameRecord> metricRecords = this.aggregationService.fetchMetricNames();
            return new ResponseEntity<>(metricRecords, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
