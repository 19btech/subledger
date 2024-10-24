package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.Aggregation;
import com.reserv.dataloader.service.aggregation.AggregationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

@RestController
@RequestMapping("/api/dataloader/aggregation")
@Slf4j
public class AggregationController {
    private final AggregationService aggregationService;

    @Autowired
    public AggregationController(AggregationService aggregationService) {
        this.aggregationService = aggregationService;
    }


    @PostMapping("/add")
    public void saveDate(@RequestBody Aggregation t) {
        this.aggregationService.save(t);
    }

    @GetMapping("/get/all")
    public ResponseEntity<Collection<Aggregation>> getAllAggregates() {
        try {
            Collection<Aggregation> transactions = this.aggregationService.fetchAll();
            return new ResponseEntity<>(transactions, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
