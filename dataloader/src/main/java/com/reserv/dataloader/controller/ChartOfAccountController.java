package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.entity.ChartOfAccount;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.repository.AccountTypesRepository;
import com.fyntrac.common.repository.AttributesRepository;
import com.fyntrac.common.repository.ChartOfAccountRepository;
import com.fyntrac.common.utils.NumberUtil;
import com.reserv.dataloader.validation.ChartOfAccountValidator;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.validation.ChartOfAccountKey;
import com.fyntrac.common.enums.ErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/dataloader/chartofaccount")
@Slf4j
public class ChartOfAccountController {
    private final DataService dataService;
    private final AccountTypesRepository accountTypesRepository;
    private final AttributesRepository attributesRepository;
    private final ChartOfAccountRepository chartOfAccountRepository;

    @Autowired
    public ChartOfAccountController(DataService dataService, AccountTypesRepository accountTypesRepository, AttributesRepository attributesRepository, ChartOfAccountRepository chartOfAccountRepository) {
        this.dataService = dataService;
        this.accountTypesRepository = accountTypesRepository;
        this.attributesRepository = attributesRepository;
        this.chartOfAccountRepository = chartOfAccountRepository;
    }


    @PostMapping("/add")
    public ResponseEntity<?> saveDate(@RequestBody ChartOfAccount t) {
        if (t == null) {
            return ResponseEntity.badRequest().body("Request body is required.");
        }

        // Normalize before validating/saving so a pasted-in "1000.0" is stored as "1000".
        t.setAccountNumber(NumberUtil.normalizeWholeNumberString(t.getAccountNumber()));

        try {
            // Instantiate validator dynamically to avoid StepScope lookup issues in HTTP threads
            ChartOfAccountValidator validator = new ChartOfAccountValidator(accountTypesRepository, attributesRepository);
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

        // DB-level duplicate check — the validator's own in-memory set can never catch
        // cross-request duplicates on this REST path (a fresh validator instance is
        // constructed per request, see comment above).
        //
        // A record is a duplicate only when accountNumber + accountName + accountSubtype AND
        // every custom attribute value match an existing row. The triple on its own is not
        // unique: the same number/name/subtype may repeat with a different attribute
        // combination, which is how one account maps to several attribute sets.
        //
        // The previous second rule — rejecting a subtype + attribute-set that was already used
        // under a different account number or name — has been removed; that combination is now
        // allowed. Keys are built by ChartOfAccountKey so this path and the batch upload
        // validator cannot drift apart.
        try {
            Collection<ChartOfAccount> existingList = dataService.fetchAllData(ChartOfAccount.class);
            if (existingList != null) {
                String myKey = ChartOfAccountKey.of(t);

                for (ChartOfAccount existing : existingList) {
                    if (t.getId() != null && t.getId().equals(existing.getId())) {
                        continue; // exclude self on edit
                    }
                    if (myKey.equals(ChartOfAccountKey.of(existing))) {
                        List<ItemValidationException.ValidationError> errors = new ArrayList<>();
                        errors.add(new ItemValidationException.ValidationError(
                                "composite", ChartOfAccountKey.describe(t),
                                ErrorCode.ERR_DUP_01.getCode(),
                                "Duplicate chart of account entry found. All fields match an existing record.",
                                "ERROR"));
                        return ResponseEntity.badRequest().body(errors);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed database duplicate validation check for ChartOfAccount", e);
        }

        dataService.save(t);
        return ResponseEntity.ok("Chart of account saved successfully.");
    }


    @GetMapping("/get/all")
    public ResponseEntity<Collection<ChartOfAccount>> getAll() {
        try {
            Collection<ChartOfAccount> collection = chartOfAccountRepository.findByIsDeletedFalse();
            return new ResponseEntity<>(collection, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<Void> deleteChartOfAccountById(@PathVariable String id) {
        try {
            long modifiedCount = chartOfAccountRepository.softDeleteById(id);
            if (modifiedCount == 0) {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } catch (Exception e) {
            log.error("Error deleting chart of account by ID [{}]: {}", id, e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}

