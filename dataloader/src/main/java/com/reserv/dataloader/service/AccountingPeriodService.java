package com.reserv.dataloader.service;

import com.reserv.dataloader.accounting.AccountingPeriodGenerator;
import com.reserv.dataloader.config.ReferenceData;
import com.reserv.dataloader.entity.AccountingPeriod;
import com.reserv.dataloader.entity.Settings;
import com.reserv.dataloader.repository.MemcachedRepository;
import com.reserv.dataloader.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AccountingPeriodService extends CacheBasedService<AccountingPeriod>{

    @Autowired
    public AccountingPeriodService(DataService dataService
                                    ,MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
    }

    @Override
    public void save(AccountingPeriod accountingPeriod) {
        this.dataService.save(accountingPeriod);
    }


    @Override
    public Collection<AccountingPeriod> fetchAll() {
        return dataService.fetchAllData(AccountingPeriod.class);
    }

    @Override
    public void loadIntoCache() {

            AccountingPeriod previoudAccountingPeriod = getPreviousAccountingPeriod();
            AccountingPeriod currentAccountingPeriod = getCurrentAccountingPeriod();
            int currentAccountingPeriodId = currentAccountingPeriod == null ? 0 : currentAccountingPeriod.getPeriodId();
            int previousAccountingPeriodId = previoudAccountingPeriod == null ? 0 : previoudAccountingPeriod.getPeriodId();

            ReferenceData referenceData = this.memcachedRepository.getFromCache(this.dataService.getTenantId(), ReferenceData.class);
            if(referenceData == null) {
                referenceData = ReferenceData.builder()
                        .previoudAccountingPeriodId(previousAccountingPeriodId)
                        .currentAccountingPeriodId(currentAccountingPeriodId)
                        .build();
            }else {
                referenceData.setPrevioudAccountingPeriodId(previousAccountingPeriodId);
                referenceData.setCurrentAccountingPeriodId(currentAccountingPeriodId);
            }
            this.memcachedRepository.putInCache(this.dataService.getTenantId() ,referenceData);

            this.loadIntoCache(Boolean.TRUE);
    }

    public void loadIntoCache(boolean forceRefresh) {
        if(this.memcachedRepository.ifExists(Key.accountingPeriodKey(this.dataService.getTenantId()))) {
            if(forceRefresh) {
                this.memcachedRepository.delete(Key.accountingPeriodKey(this.dataService.getTenantId()));
            }else{
                return;
            }
        }

        this.memcachedRepository.putInCache(Key.accountingPeriodKey(this.dataService.getTenantId()), this.getAccountingPeriodsMap());
    }
    private AccountingPeriod getCurrentAccountingPeriod() {
        Query query = new Query();
        query.addCriteria(Criteria.where("status").is(0));
        query.with(Sort.by(Sort.Direction.ASC, "periodId"));
        query.limit(1);
        List<AccountingPeriod> accountingPeriods =  dataService.fetchData(query, AccountingPeriod.class);
        if(accountingPeriods == null || accountingPeriods.isEmpty()) {
                return null;
        }
        return accountingPeriods.get(0);
    }

    private Map<String, AccountingPeriod> getAccountingPeriodsMap() {
        List<AccountingPeriod> accountingPeriods = getAccountingPeriods();
        Map<String, AccountingPeriod> accountingPeriodMap = new HashMap<>();
        for(AccountingPeriod ap : accountingPeriods) {
            accountingPeriodMap.put(ap.getPeriod(), ap);
        }
        return accountingPeriodMap;
    }

    private List<AccountingPeriod> getAccountingPeriods() {
        Query query = new Query();
        query.addCriteria(Criteria.where("status").is(0));
        query.with(Sort.by(Sort.Direction.ASC, "periodId"));
        List<AccountingPeriod> accountingPeriods =  dataService.fetchData(query, AccountingPeriod.class);
        if(accountingPeriods == null || accountingPeriods.isEmpty()) {
            return null;
        }
        return accountingPeriods;
    }

    private AccountingPeriod getPreviousAccountingPeriod() {
        Query query = new Query();
        query.addCriteria(Criteria.where("status").is(1));
        query.with(Sort.by(Sort.Direction.DESC, "periodId"));
        query.limit(1);
        List<AccountingPeriod> accountingPeriods =  dataService.fetchData(query, AccountingPeriod.class);
        if(accountingPeriods == null || accountingPeriods.isEmpty()) {
            return null;
        }
        return accountingPeriods.get(0);
    }

    public Collection<AccountingPeriod> generateAccountingPeriod(Settings s) throws ParseException {
        AccountingPeriodGenerator accountingPeriodGenerator = new AccountingPeriodGenerator();
        Set<AccountingPeriod> accountingPeriods = accountingPeriodGenerator.generate(s.getFiscalPeriodStartDate());

        return dataService.saveAll(accountingPeriods, AccountingPeriod.class);
    }

    public void updateAccountingPeriodStatus(int status) {
        Update update = new Update().set("status", status);
        dataService.update(new Query(Criteria.where("status").is(1)), update, AccountingPeriod.class);
    }

    public void reopenAccountingPeriods(String accountingPeriod) {
        Update update = new Update().set("status", 0);
        String[] ap = accountingPeriod.split("-");
        String accountingPeriodId = "";

        for(String s : ap) {
            accountingPeriodId = accountingPeriodId + s.replace("\"","");
        }

        dataService.update(new Query(Criteria.where("status").is(1).andOperator(Criteria.where("periodId").gte(Integer.parseInt(accountingPeriodId)))), update, AccountingPeriod.class);
    }

    public Collection<String> getClosedAccountingPeriods() {
        Sort sort = Sort.by(Sort.Direction.ASC, "startDate");
        Query query = new Query(Criteria.where("status").is(1)).with(sort).limit(1);
        query.fields().include("period");
        List<AccountingPeriod> accountingPeriods = dataService.fetchData(query, AccountingPeriod.class);
        return accountingPeriods.stream().map(AccountingPeriod::getPeriod).collect(Collectors.toList());
    }

}
