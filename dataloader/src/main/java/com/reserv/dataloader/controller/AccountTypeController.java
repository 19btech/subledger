package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.service.DataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

@RestController
@RequestMapping("/api/dataloader/accounttype")
@Slf4j
public class AccountTypeController {
    private final DataService dataService;

    @Autowired
    public AccountTypeController(DataService dataService) {
        this.dataService = dataService;
    }


    @PostMapping("/add")
    public void saveDate(@RequestBody AccountTypes t) {
        dataService.save(t);
    }

    @GetMapping("/get/all")
    public ResponseEntity<Collection<AccountTypes>> getAllAggregates() {
        try {
            Collection<AccountTypes> transactions = dataService.fetchAllData(AccountTypes.class);
            return new ResponseEntity<>(transactions, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/subtypes")
    public ResponseEntity<String[]> getAllAccountSubtypes() {
        try {
            Collection<AccountTypes> collection = dataService.findByColumns(AccountTypes.class, "accountSubType", "accountType");
            String[] accountSubtypes = collection.stream()
                    .map(AccountTypes::getAccountSubType) // Replace getFieldName with the actual getter method for your field
                    .toArray(String[]::new);
            return new ResponseEntity<>(accountSubtypes, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
