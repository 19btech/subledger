package com.reserv.dataloader.service;

import com.reserv.dataloader.accounting.AccountingPeriodGenerator;
import  com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.Settings;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import com.fyntrac.common.service.DataService;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.BatchService;

@Service
@Slf4j
public class AccountingPeriodDataUploadService extends AccountingPeriodService {

    @Autowired
    public AccountingPeriodDataUploadService(DataService dataService
                                    , MemcachedRepository memcachedRepository, BatchService batchService)  {
        super(dataService, memcachedRepository, batchService);
    }

    public Collection<AccountingPeriod> generateAccountingPeriod(Settings s) throws ParseException {
        AccountingPeriodGenerator accountingPeriodGenerator = new AccountingPeriodGenerator();
        this.dataService.truncateCollection(AccountingPeriod.class);
        Set<AccountingPeriod> accountingPeriods = accountingPeriodGenerator.generate(s.getFiscalPeriodStartDate());

        return dataService.saveAll(accountingPeriods, AccountingPeriod.class);
    }

}
