package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.Option;
import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.service.TransactionService;
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

    private final TransactionService transactionService;
    private final com.reserv.dataloader.validation.TransactionValidator transactionValidator;

    @Autowired
    public TransactionController(TransactionService transactionService, com.reserv.dataloader.validation.TransactionValidator transactionValidator) {
        this.transactionService = transactionService;
        this.transactionValidator = transactionValidator;
    }

    @PostMapping("/add")
    public ResponseEntity<?> saveData(@RequestBody Transactions t) {
        if (t == null) {
            return ResponseEntity.badRequest().body("Request body is required.");
        }

        // Leverage efficient single-record existence check for fast REST validation
        java.util.Set<String> existingNames = new java.util.HashSet<>();
        if (t.getName() != null && transactionService != null) {
            String txName = t.getName().trim();
            Transactions existing = transactionService.getTransaction(txName);
            if (existing != null) {
                // Pick object from validation only where ID is NOT equal to request object (Updation logic)
                if (t.getId() == null || !existing.getId().equals(t.getId())) {
                    existingNames.add(txName.toUpperCase());
                }
            }
        }

        // Invoke the unified stateless validation matrix
        java.util.List<com.reserv.dataloader.batch.exception.ItemValidationException.ValidationError> errors = 
                transactionValidator.validate(t, existingNames);
        
        boolean hasError = errors.stream().anyMatch(log -> "ERROR".equals(log.getSeverity()));

        if (hasError) {
            return ResponseEntity.badRequest().body(errors);
        }

        transactionService.save(t);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/get/all")
    public ResponseEntity<Collection<Transactions>> getAllTransactions() {
        try {
            Collection<Transactions> transactions = transactionService.getAll();
            return new ResponseEntity<>(transactions, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/all/options")
    public ResponseEntity<Collection<Option>> getTransactionOptions() {
        try {
            Collection<Option> transactions = transactionService.fetchTransactionOptions();
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
            Collection<Transactions> collection = transactionService.getTransactionNames();
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

    @GetMapping("/get/transactionNames")
    public ResponseEntity<Collection<Records.TransactionNameRecord>> getTransactionNames() {
        try {
            Collection<Records.TransactionNameRecord> transactions = transactionService.fetchTransactinNames();
            return new ResponseEntity<>(transactions, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
