package com.fyntrac.common.service;

import com.fyntrac.common.cache.collection.CacheMap;
import  com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.Batch;
import com.fyntrac.common.enums.BatchStatus;
import com.fyntrac.common.enums.BatchType;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AccountingPeriodService extends CacheBasedService<AccountingPeriod>{


    private final BatchService batchService;
    @Autowired
    public AccountingPeriodService(DataService dataService
            ,MemcachedRepository memcachedRepository, BatchService batchService) {
        super(dataService, memcachedRepository);
        this.batchService = batchService;
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
    public void loadIntoCache() throws ExecutionException, InterruptedException {

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

    public void loadIntoCache(boolean forceRefresh) throws ExecutionException, InterruptedException {
        String tenantId = this.dataService.getTenantId();
        String cacheKey = Key.accountingPeriodKey(tenantId);

        // Check if the cache entry exists
        if (this.memcachedRepository.ifExists(cacheKey)) {
            // If forceRefresh is true, delete the existing cache entry
            if (forceRefresh) {
                this.memcachedRepository.delete(cacheKey);
            } else {
                // If not forcing a refresh, exit early
                return;
            }
        }

        // Load the accounting periods into the cache
        this.memcachedRepository.putInCache(cacheKey, this.getAccountingPeriodsMap());
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

    public AccountingPeriod getAccountingPeriod(int periodId) {
        return this.getAccountingPeriod(periodId, this.dataService.getTenantId());
    }

    public AccountingPeriod getAccountingPeriod(int periodId, String tenantId) {

        String key = Key.accountingPeriodKey(tenantId);
        CacheMap<AccountingPeriod> accountingPeriodCacheMap = this.memcachedRepository.getFromCache(key, CacheMap.class);
        if(accountingPeriodCacheMap == null) {
            accountingPeriodCacheMap = new CacheMap<>();
            AccountingPeriod accountingPeriod = this.getPeriod(periodId, tenantId);
            accountingPeriodCacheMap.put(String.valueOf(accountingPeriod.getPeriodId()), accountingPeriod);
            this.memcachedRepository.putInCache(key, accountingPeriodCacheMap);
            return accountingPeriod;
        }

        AccountingPeriod accountingPeriod = accountingPeriodCacheMap.getValue(String.valueOf(periodId));
        if(accountingPeriod == null) {
            accountingPeriod = this.getPeriod(periodId, tenantId);
            accountingPeriodCacheMap.put(String.valueOf(accountingPeriod.getPeriodId()), accountingPeriod);
            this.memcachedRepository.putInCache(key, accountingPeriodCacheMap);
        }

        return accountingPeriod;
    }

    public AccountingPeriod getPeriod(int periodId, String tenantId) {
        CacheMap<AccountingPeriod> accountingPeriodCacheMap;
        Query query = new Query();
        query.addCriteria(Criteria.where("periodId").is(periodId));
        return this.dataService.findOne(query, tenantId, AccountingPeriod.class);
    }
    private CacheMap<AccountingPeriod> getAccountingPeriodsMap() {
        Collection<AccountingPeriod> accountingPeriods = getAccountingPeriods();
        CacheMap<AccountingPeriod> accountingPeriodMap = new CacheMap<>();
        for(AccountingPeriod ap : accountingPeriods) {
            accountingPeriodMap.put(String.valueOf(ap.getPeriodId()), ap);
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

    public void updateAccountingPeriodStatus(int status) {
        Update update = new Update().set("status", status);
        dataService.update(new Query(Criteria.where("status").is(1)), update, AccountingPeriod.class);
    }

    public void closeAccountingPeriod(int accountingPeriodId) {
        try {
            //fetch pending batches and create pulsar message to generate GLE
            Collection<Batch> batches = this.batchService.getBatchesByTypeAndStatus(BatchType.ACTIVITY, BatchStatus.PENDING);

            Update update = new Update().set("status", 1);
            Criteria criteria = Criteria.where("status").is(0);
            criteria.and("periodId").lte(accountingPeriodId);
            dataService.update(new Query(criteria), update, AccountingPeriod.class);

        }catch (Exception exp){
            log.error(exp.getLocalizedMessage());
            throw new RuntimeException(exp.getLocalizedMessage());
        }
    }


    public Records.AccountingPeriodCloseMessageRecord generateAccountingPeriodCloseRecord() {
        try {
            //fetch pending batches and create pulsar message to generate GLE
            Collection<Batch> batches = this.batchService.getBatchesByTypeAndStatus(BatchType.ACTIVITY, BatchStatus.PENDING);
            return RecordFactory.createAccountingPeriodCloseMessage(this.dataService.getTenantId(), batches);
        }catch (Exception exp){
            log.error(exp.getLocalizedMessage());
            throw new RuntimeException(exp.getLocalizedMessage());
        }
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

