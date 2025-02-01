package com.fyntrac.common.service;

import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.Settings;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.accounting.AccountingPeriodGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.Collection;
import java.util.Set;

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
