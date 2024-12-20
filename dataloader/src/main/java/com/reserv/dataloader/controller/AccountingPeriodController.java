package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.reserv.dataloader.pulsar.producer.AccountingPeriodCloseProducer;
import com.reserv.dataloader.service.AccountingPeriodDataUploadService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import com.fyntrac.common.dto.record.Records.AccountingPeriodRecord;

@Slf4j
@RestController
@RequestMapping("/api/dataloader/accounting-period")
public class AccountingPeriodController {

    private AccountingPeriodDataUploadService accountingPeriodService;
    private AccountingPeriodCloseProducer accountingPeriodCloseProducer;

    @Autowired
    public AccountingPeriodController(AccountingPeriodDataUploadService accountingPeriodService
    , AccountingPeriodCloseProducer accountingPeriodCloseProducer) {
        this.accountingPeriodService=accountingPeriodService;
        this.accountingPeriodCloseProducer = accountingPeriodCloseProducer;
    }

    @GetMapping("/get/open-periods")
    public ResponseEntity<Collection<Records.AccountingPeriodRecord>> getOpenAccountingPeriods(){
        try {
            Collection<AccountingPeriod> accountingPeriods = this.accountingPeriodService.getAccountingPeriods();
            List<Records.AccountingPeriodRecord> accountingPeriodRecords =  accountingPeriods.stream()
                    .map(RecordFactory::createAccountingPeriodRecord)
                    .sorted(Comparator.comparingInt(Records.AccountingPeriodRecord::periodId))
                    .collect(Collectors.toList());

            return new ResponseEntity<>(accountingPeriodRecords, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/last-closed-period")
    public ResponseEntity<Records.AccountingPeriodRecord> getLastClosedAccountingPeriod(){
        try {
            AccountingPeriod accountingPeriod = this.accountingPeriodService.getLastClosedAccountingPeriod();
            Records.AccountingPeriodRecord lastClosedAccountingPeriod =  RecordFactory.createAccountingPeriodRecord(accountingPeriod);

            return new ResponseEntity<>(lastClosedAccountingPeriod, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/current-open-period")
    public ResponseEntity<Records.AccountingPeriodRecord> getCurrentOpenAccountingPeriod(){
        try {
            AccountingPeriod lastClosedAccountingPeriod =  accountingPeriodService.getLastClosedAccountingPeriod();
            Records.AccountingPeriodRecord currentPeriod = null;
            if(lastClosedAccountingPeriod == null) {
                currentPeriod = RecordFactory.createAccountingPeriodRecord(null);
            }else{
                AccountingPeriod currentOpenAccountingPeriod = this.accountingPeriodService.getCurrentAccountingPeriod();
                currentPeriod = RecordFactory.createAccountingPeriodRecord(currentOpenAccountingPeriod);
            }
            return new ResponseEntity<>(currentPeriod, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/close")
    public ResponseEntity<Records.AccountingPeriodRecord> saveDate(@RequestBody AccountingPeriodRecord accountingPeriod) throws ParseException {
        try {
            Records.AccountingPeriodCloseMessageRecord accountingPeriodCloseMessageRecord = accountingPeriodService.generateAccountingPeriodCloseRecord();
            accountingPeriodCloseProducer.closeAccountingPeriod(accountingPeriodCloseMessageRecord);
        }catch (Exception exp){
            log.error(exp.getLocalizedMessage());
            throw new RuntimeException(exp.getLocalizedMessage());
        }
        return new ResponseEntity<>(accountingPeriod, HttpStatus.OK);
    }

}
