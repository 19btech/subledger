package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.ChartOfAccount;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.repository.AccountTypesRepository;
import com.reserv.dataloader.validation.ChartOfAccountValidator;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/dataloader/chartofaccount")
@Slf4j
public class ChartOfAccountController {
    private final DataService dataService;
    private final AccountTypesRepository accountTypesRepository;

    @Autowired
    public ChartOfAccountController(DataService dataService, AccountTypesRepository accountTypesRepository) {
        this.dataService = dataService;
        this.accountTypesRepository = accountTypesRepository;
    }


    @PostMapping("/add")
    public ResponseEntity<?> saveDate(@RequestBody ChartOfAccount t) {
        if (t == null) {
            return ResponseEntity.badRequest().body("Request body is required.");
        }

        try {
            // Instantiate validator dynamically to avoid StepScope lookup issues in HTTP threads
            ChartOfAccountValidator validator = new ChartOfAccountValidator(accountTypesRepository);
            validator.init();

            Map<String, Object> rawData = new HashMap<>();
            rawData.put("ACCOUNTNUMBER", t.getAccountNumber());
            rawData.put("ACCOUNTNAME", t.getAccountName());
            rawData.put("ACCOUNTSUBTYPE", t.getAccountSubtype());
            if (t.getAttributes() != null) {
                rawData.putAll(t.getAttributes());
            }

            validator.validate(t, rawData);
        } catch (ItemValidationException e) {
            return ResponseEntity.badRequest().body(e.getValidationErrors());
        } catch (Exception e) {
            log.error("Failed to validate ChartOfAccount record", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("An error occurred during verification.");
        }

        dataService.save(t);
        return ResponseEntity.ok("Chart of account saved successfully.");
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

