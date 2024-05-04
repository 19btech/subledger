package com.reserv.dataloader.controller;

import com.reserv.dataloader.entity.Transactions;
import com.reserv.dataloader.service.DataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

@RestController
@RequestMapping("/api/dataloader/transaction")
@Slf4j
public class TransactionController {

    private final DataService dataService;

    @Autowired
    public TransactionController(DataService dataService) {
        this.dataService = dataService;
    }

    @PostMapping("/add")
    public void saveData(@RequestBody Transactions t) {
        dataService.save(t);
    }

    @GetMapping("/get/all")
    public ResponseEntity<Collection<Transactions>> getAllTransactions() {
        try {
            Collection<Transactions> transactions = dataService.fetchAllData(Transactions.class);
            return new ResponseEntity<>(transactions, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/transactions")
    public ResponseEntity<String[]> getAllTransactionNames() {
        try {
            Collection<Transactions> collection = dataService.findByColumns(Transactions.class, "name");
            String[] transactions = collection.stream()
                    .map(Transactions::getName) // Replace getFieldName with the actual getter method for your field
                    .toArray(String[]::new);
            return new ResponseEntity<>(transactions, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
