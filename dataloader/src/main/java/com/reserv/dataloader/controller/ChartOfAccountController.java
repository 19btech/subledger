package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.ChartOfAccount;
import com.reserv.dataloader.service.DataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

@RestController
@RequestMapping("/api/dataloader/chartofaccount")
@Slf4j
public class ChartOfAccountController {
    private final DataService dataService;

    @Autowired
    public ChartOfAccountController(DataService dataService) {
        this.dataService = dataService;
    }


    @PostMapping("/add")
    public void saveDate(@RequestBody ChartOfAccount t) {
        dataService.save(t);
    }

    @GetMapping("/get/all")
    public ResponseEntity<Collection<ChartOfAccount>> getAll() {
        try {
            Collection<ChartOfAccount> collection = dataService.fetchAllData(ChartOfAccount.class);
            return new ResponseEntity<>(collection, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}

