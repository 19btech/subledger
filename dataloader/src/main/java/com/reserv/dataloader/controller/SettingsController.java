package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.DashboardConfiguration;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.entity.Settings;
import com.fyntrac.common.enums.Currency;
import com.fyntrac.common.service.ExecutionStateService;
import com.reserv.dataloader.repository.ActivityLogRepo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.fyntrac.common.service.SettingsService;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/dataloader/setting")
@Slf4j
public class SettingsController {

    private final SettingsService settingsService ;
    private final ActivityLogRepo activityLogRepo;
    private final ExecutionStateService executionStateService;

    @Autowired
    public SettingsController(SettingsService settingsService, ActivityLogRepo activityLogRepo,
                               ExecutionStateService executionStateService) {
        this.settingsService = settingsService;
        this.activityLogRepo = activityLogRepo;
        this.executionStateService = executionStateService;
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
    public ResponseEntity<Settings> getSettings() {
        try {
            Settings settings = settingsService.fetch();
            return new ResponseEntity<>(settings, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/currencies")
    public ResponseEntity<List<String>> getCurrencies() {
        try {
            List<String> currencyList = Arrays.stream(Currency.values())
                    .map(Currency::getCode)
                    .sorted()
                    .toList();
            return new ResponseEntity<>(currencyList, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/save/currency")
    public ResponseEntity saveCurrency(@RequestBody String currency) {
        try {
            String cleanCode = currency.replaceAll("^\"|\"$", "");
            if(cleanCode != null && !cleanCode.isEmpty()){
                settingsService.saveCurrency(cleanCode);
            }
            return new ResponseEntity(HttpStatus.OK);
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

    @PostMapping("/dashboard-configuration/save")
    public ResponseEntity<Boolean> saveDashboardConfiguration(@RequestBody DashboardConfiguration dc) {
        try{
            DashboardConfiguration dashboardConfiguration = this.settingsService.saveDashboardConfiguration(dc);
            return new ResponseEntity<>(Boolean.TRUE,HttpStatus.OK);
        }catch (Exception e){
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Permanently deletes all activity data (raw activity, derived balances/GL entries, and
     * validation logs) posted on the latest loaded posting date, plus the matching ActivityLog
     * load record(s) and the ExecutionState record(s) tracking that date. This is irreversible.
     * <p>
     * The posting date is never taken from the caller - it is always the tenant's current
     * {@link ExecutionState#getExecutionDate()}, i.e. the same value the Settings page displays
     * as the "latest loaded posting date". This keeps what is shown and what gets deleted in
     * sync, and rules out deleting the wrong date because of a stale UI value.
     */
    @PostMapping("/delete/activity-data")
    public ResponseEntity<?> deleteActivityData() {
        try {
            ExecutionState executionState = executionStateService.getExecutionState();
            Integer postingDate = (executionState != null) ? executionState.getExecutionDate() : null;
            if (postingDate == null || postingDate == 0) {
                return ResponseEntity.badRequest().body("No activity data has been loaded yet.");
            }

            Map<String, Long> deletedCounts = settingsService.deleteActivityData(postingDate);
            long activityLogsDeleted = activityLogRepo.deleteByPostingDate(postingDate);
            deletedCounts.put("ActivityLog", activityLogsDeleted);

            // Roll the ExecutionState back to whatever preceded this posting date, so the
            // "latest loaded posting date" reflects reality again right after the delete.
            executionStateService.rollbackToBeforeDate(postingDate);

            log.warn("Activity data deletion for posting date [{}] complete: {}", postingDate, deletedCounts);
            return new ResponseEntity<>(deletedCounts, HttpStatus.OK);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        } catch (Exception e) {
            log.error("Failed to delete activity data: {}", e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
