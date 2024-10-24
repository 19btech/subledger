package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.SubledgerMapping;
import com.reserv.dataloader.service.DataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

@RestController
@RequestMapping("/api/dataloader/subledgermapping")
@Slf4j
public class SubledgerMappingController {
    private final DataService dataService;

    @Autowired
    public SubledgerMappingController(DataService dataService) {
        this.dataService = dataService;
    }


    @PostMapping("/add")
    public void saveDate(@RequestBody SubledgerMapping t) {
        dataService.save(t);
    }

    @GetMapping("/get/all")
    public ResponseEntity<Collection<SubledgerMapping>> getAllAggregates() {
        try {
            Collection<SubledgerMapping> collection = dataService.fetchAllData(SubledgerMapping.class);
            return new ResponseEntity<>(collection, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}

