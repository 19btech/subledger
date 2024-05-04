package com.reserv.dataloader.controller;

import com.reserv.dataloader.entity.Aggregation;
import com.reserv.dataloader.service.DataService;
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
    private final DataService dataService;

    @Autowired
    public AggregationController(DataService dataService) {
        this.dataService = dataService;
    }


    @PostMapping("/add")
    public void saveDate(@RequestBody Aggregation t) {
        dataService.save(t);
    }

    @GetMapping("/get/all")
    public ResponseEntity<Collection<Aggregation>> getAllAggregates() {
        try {
            Collection<Aggregation> transactions = dataService.fetchAllData(Aggregation.class);
            return new ResponseEntity<>(transactions, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
