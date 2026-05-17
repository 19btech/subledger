package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.SubledgerMapping;
import com.fyntrac.common.service.DataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.fyntrac.common.repository.TransactionsRepository;
import com.fyntrac.common.repository.AccountTypesRepository;
import com.reserv.dataloader.validation.SubledgerMappingValidator;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.fyntrac.common.enums.ErrorCode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Collection;

@RestController
@RequestMapping("/api/dataloader/subledgermapping")
@Slf4j
public class SubledgerMappingController {
    private final DataService dataService;
    private final SubledgerMappingValidator subledgerMappingValidator;
    private final TransactionsRepository transactionsRepository;
    private final AccountTypesRepository accountTypesRepository;

    @Autowired
    public SubledgerMappingController(
            DataService dataService,
            SubledgerMappingValidator subledgerMappingValidator,
            TransactionsRepository transactionsRepository,
            AccountTypesRepository accountTypesRepository) {
        this.dataService = dataService;
        this.subledgerMappingValidator = subledgerMappingValidator;
        this.transactionsRepository = transactionsRepository;
        this.accountTypesRepository = accountTypesRepository;
    }


    @PostMapping("/add")
    public ResponseEntity<?> saveDate(@RequestBody SubledgerMapping t) {
        if (t == null) {
            return ResponseEntity.badRequest().body("Request body is required.");
        }

        // 1. Build preloaded sets for transaction names
        Set<String> validTxNames = new HashSet<>();
        transactionsRepository.findAll().forEach(tx -> {
            if (tx.getName() != null) {
                validTxNames.add(tx.getName().trim());
            }
        });

        // 2. Build preloaded sets for account sub types
        Set<String> validAccSubTypes = new HashSet<>();
        accountTypesRepository.findAll().forEach(acc -> {
            if (acc.getAccountSubType() != null) {
                validAccSubTypes.add(acc.getAccountSubType().trim());
            }
        });

        // 3. Clear validator state and validate individual/composite rules in memory
        subledgerMappingValidator.clearState();
        try {
            subledgerMappingValidator.validate(t, validTxNames, validAccSubTypes);
        } catch (ItemValidationException e) {
            return ResponseEntity.badRequest().body(e.getValidationErrors());
        }

        // 4. Check composite duplicate against DB (Rule 2: transactionName + sign + accountSubType)
        // Excluding own record on update (if t.getId() is provided)
        try {
            Collection<SubledgerMapping> existingList = dataService.fetchAllData(SubledgerMapping.class);
            if (existingList != null && t.getTransactionName() != null && t.getSign() != null && t.getAccountSubType() != null) {
                String txName = t.getTransactionName().trim().toUpperCase();
                String sign = t.getSign().name().toUpperCase();
                String subType = t.getAccountSubType().trim().toUpperCase();
                for (SubledgerMapping existing : existingList) {
                    if (existing.getTransactionName() != null && existing.getSign() != null && existing.getAccountSubType() != null) {
                        if (existing.getTransactionName().trim().equalsIgnoreCase(txName)
                                && existing.getSign().name().equalsIgnoreCase(sign)
                                && existing.getAccountSubType().trim().equalsIgnoreCase(subType)) {
                            // Exclude self on update
                            if (t.getId() == null || !existing.getId().equals(t.getId())) {
                                List<ItemValidationException.ValidationError> errors = new ArrayList<>();
                                errors.add(new ItemValidationException.ValidationError(
                                        "composite",
                                        t.getTransactionName() + "|" + t.getSign() + "|" + t.getAccountSubType(),
                                        ErrorCode.ERR_DUP_02.getCode(),
                                        "Duplicate rule: Subledger mapping with this Transaction Name, Sign, and Account Subtype already exists in database.",
                                        "ERROR"
                                ));
                                return ResponseEntity.badRequest().body(errors);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed database duplicate validation check", e);
        }

        dataService.save(t);
        return ResponseEntity.ok().build();
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

