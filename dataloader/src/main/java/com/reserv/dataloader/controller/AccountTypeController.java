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
    private final com.reserv.dataloader.validation.AccountTypesValidator validator;

    @Autowired
    public AccountTypeController(DataService dataService, com.reserv.dataloader.validation.AccountTypesValidator validator) {
        this.dataService = dataService;
        this.validator = validator;
    }


    @PostMapping("/add")
    public ResponseEntity<?> saveDate(@RequestBody AccountTypes t) {
        if (t == null) {
            return ResponseEntity.badRequest().body("Request body is required.");
        }

        // Build preloaded structures for validation against DB records
        java.util.Set<String> seenSubTypes = new java.util.HashSet<>();
        java.util.Map<String, String> subTypeToTypeMap = new java.util.HashMap<>();

        if (t.getAccountSubType() != null) {
            String subType = t.getAccountSubType().trim();
            try {
                // Fetch existing records to check for duplicates and multi-mapping violations
                Collection<AccountTypes> existingList = dataService.fetchAllData(AccountTypes.class);
                if (existingList != null) {
                    for (AccountTypes existing : existingList) {
                        if (existing.getAccountSubType() != null && existing.getAccountSubType().equalsIgnoreCase(subType)) {
                            // 1. Detect duplicates (excluding the current record if it's an update)
                            if (t.getId() == null || !existing.getId().equals(t.getId())) {
                                seenSubTypes.add(subType.toUpperCase());
                            }
                            // 2. Track mappings to detect if same subtype maps to different types
                            if (existing.getAccountType() != null) {
                                subTypeToTypeMap.put(subType.toUpperCase(), existing.getAccountType().name());
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Failed to fetch existing AccountTypes for validation", e);
            }
        }

        java.util.List<com.reserv.dataloader.batch.exception.ItemValidationException.ValidationError> errors = 
                validator.validate(t, seenSubTypes, subTypeToTypeMap);
        
        boolean hasError = errors.stream().anyMatch(err -> "ERROR".equals(err.getSeverity()));

        if (hasError) {
            return ResponseEntity.badRequest().body(errors);
        }

        dataService.save(t);
        return ResponseEntity.ok().build();
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
