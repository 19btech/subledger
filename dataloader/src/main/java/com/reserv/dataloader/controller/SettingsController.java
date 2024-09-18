package com.reserv.dataloader.controller;

import com.reserv.dataloader.accounting.AccountingPeriodGenerator;
import com.reserv.dataloader.entity.Settings;
import com.reserv.dataloader.entity.Transactions;
import com.reserv.dataloader.service.DataService;
import com.reserv.dataloader.service.SettingsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.util.Collection;
import java.util.Date;

@RestController
@RequestMapping("/api/dataloader/setting")
@Slf4j
public class SettingsController {

    private final SettingsService settingsService ;

    @Autowired
    public SettingsController(SettingsService settingsService) {
        this.settingsService = settingsService;
    }

    @PostMapping("/fiscal-priod/save")
    public ResponseEntity<Settings> saveDate(@RequestBody Settings s) {

        try {
            Settings settings = settingsService.saveFiscalPriod(s.getFiscalPeriodStartDate());
            settingsService.generateAccountingPeriod(settings);
            return new ResponseEntity<>(settings, HttpStatus.OK);
        } catch (ParseException e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/restatement-mode/save")
    public ResponseEntity<Settings> saveRestatementMode(@RequestBody Settings s) {
        try {
            Settings settings = settingsService.saveRestatementMode(s.getRestatementMode());
            settingsService.updateAccountingPeriodStatus(0);
            return new ResponseEntity<>(settings, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/settings")
    public ResponseEntity<Settings> getAllTransactions() {
        try {
            Settings settings = settingsService.fetch();
            return new ResponseEntity<>(settings, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/closed/accounting-periods")
    public ResponseEntity<Collection<String>> getClosedAccountingPeriods() {
        try {
            Collection<String> closedAccountingPeriods = settingsService.getClosedAccountingPeriods();
            return new ResponseEntity<>(closedAccountingPeriods, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/reopen/accounting-periods")
    public ResponseEntity<Settings> reopenAccountingPeriods(@RequestBody String accountingPeriod) {
        try {
            settingsService.reopenAccountingPeriods(accountingPeriod);
            return new ResponseEntity<>(null, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/refresh/schema")
    public ResponseEntity<Boolean> refreshSchema(@RequestBody boolean refresh) {
        try {
            if(refresh){
                settingsService.refreshSchema();
            }
            return new ResponseEntity<>(Boolean.TRUE, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
