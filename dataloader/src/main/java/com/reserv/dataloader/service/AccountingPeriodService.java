package com.reserv.dataloader.service;

import com.reserv.dataloader.accounting.AccountingPeriodGenerator;
import  com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.Settings;
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

            AccountingPeriod previoudAccountingPeriod = getLastClosedAccountingPeriod();
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
    public AccountingPeriod getCurrentAccountingPeriod() {
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
        Collection<AccountingPeriod> accountingPeriods = getAccountingPeriods();
        Map<String, AccountingPeriod> accountingPeriodMap = new HashMap<>();
        for(AccountingPeriod ap : accountingPeriods) {
            accountingPeriodMap.put(ap.getPeriod(), ap);
        }
        return accountingPeriodMap;
    }

    public Collection<AccountingPeriod> getAccountingPeriods() {
        List<AccountingPeriod> accountingPeriods =  this.getAccountingPeriods(Sort.Direction.ASC, 0);
        if(accountingPeriods == null || accountingPeriods.isEmpty()) {
            return null;
        }
        return accountingPeriods;
    }


    private List<AccountingPeriod> getAccountingPeriods(Sort.Direction sortDirection, int status) {
        Query query = new Query();
        query.addCriteria(Criteria.where("status").is(status));
        query.with(Sort.by(sortDirection, "periodId"));
        List<AccountingPeriod> accountingPeriods =  dataService.fetchData(query, AccountingPeriod.class);
        if(accountingPeriods == null || accountingPeriods.isEmpty()) {
            return null;
        }
        return accountingPeriods;
    }

    public AccountingPeriod getLastClosedAccountingPeriod() {
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
        this.dataService.truncateCollection(AccountingPeriod.class);
        Set<AccountingPeriod> accountingPeriods = accountingPeriodGenerator.generate(s.getFiscalPeriodStartDate());

        return dataService.saveAll(accountingPeriods, AccountingPeriod.class);
    }

    public void updateAccountingPeriodStatus(int status) {
        Update update = new Update().set("status", status);
        dataService.update(new Query(Criteria.where("status").is(1)), update, AccountingPeriod.class);
    }

    public void closeAccountingPeriod(int accountingPeriodId) {
        Update update = new Update().set("status", 1);
        Criteria criteria = Criteria.where("status").is(0);
        criteria.and("periodId").lte(accountingPeriodId);
        dataService.update(new Query(criteria), update, AccountingPeriod.class);
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
        // Define sorting criteria based on 'periodId'
        Sort sort = Sort.by(Sort.Direction.DESC, "periodId");

        // Define the query to fetch accounting periods with status 1 and sorted by periodId
        Query query = new Query(Criteria.where("status").is(1))
                .with(sort)
                .limit(1);

        // Include the fields you need in the query (e.g., "period" and "year" or another field)
        query.fields().include("fiscalPeriod").include("year");  // Add the second field here (e.g., "year")

        // Fetch the data from the dataService
        List<AccountingPeriod> accountingPeriods = dataService.fetchData(query, AccountingPeriod.class);

        // Concatenate the 'period' and 'year' fields, or any two fields
        return accountingPeriods.stream()
                .map(period -> period.getYear() + " / " + period.getFiscalPeriod())  // Concatenate the two fields
                .collect(Collectors.toList());
    }
}
