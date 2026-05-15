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
    private final com.reserv.dataloader.validation.AggregationValidator aggregationValidator;

    @Autowired
    public AggregationController(
            AggregationService aggregationService, 
            com.reserv.dataloader.validation.AggregationValidator aggregationValidator) {
        this.aggregationService = aggregationService;
        this.aggregationValidator = aggregationValidator;
    }


    @PostMapping("/add")
    public ResponseEntity<?> saveDate(@RequestBody Aggregation t) {
        java.util.List<com.reserv.dataloader.batch.exception.ItemValidationException.ValidationError> errors = aggregationValidator.validate(t);
        
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
