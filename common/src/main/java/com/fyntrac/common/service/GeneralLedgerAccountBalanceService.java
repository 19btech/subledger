package com.fyntrac.common.service;


import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.entity.GeneralLedgerAccountBalance;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class GeneralLedgerAccountBalanceService extends CacheBasedService<GeneralLedgerAccountBalance> {
    private String tenantId;

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
        this.dataService.setTenantId(tenantId);
    }
    @Autowired
    public GeneralLedgerAccountBalanceService(DataService<GeneralLedgerAccountBalance> dataService
            , MemcachedRepository memcachedRepository){
        super(dataService, memcachedRepository);
    }

    @Override
    public GeneralLedgerAccountBalance save(GeneralLedgerAccountBalance generalLedgerAccountBalance) {
        if(this.tenantId == null) {
            this.dataService.getTenantId();
        }

        return this.dataService.save(generalLedgerAccountBalance, tenantId);
    }

    public void saveAll(Set<GeneralLedgerAccountBalance> gleAccountsBalance) {
        this.dataService.saveAll(gleAccountsBalance, this.tenantId, GeneralLedgerAccountBalance.class);
    }

    @Override
    public Collection<GeneralLedgerAccountBalance> fetchAll() {
        return this.dataService.fetchAllData(GeneralLedgerAccountBalance.class);
    }

    public GeneralLedgerAccountBalance getGeneralLedgerAccountBalance(int hashCode) {
        String key = Key.generalLedgerAccountTypesKey(this.tenantId);
        CacheMap<GeneralLedgerAccountBalance> glAccountMap;
        if(!(this.memcachedRepository.ifExists(key))) {
            glAccountMap = new CacheMap<>();
            GeneralLedgerAccountBalance generalLedgerAccountBalance =  this.getBalance(hashCode);
            glAccountMap.put(String.valueOf(hashCode), generalLedgerAccountBalance);
            this.memcachedRepository.putInCache(key, glAccountMap);
        }
        glAccountMap = this.memcachedRepository.getFromCache(key, CacheMap.class);
        GeneralLedgerAccountBalance balance;
        balance = glAccountMap.getValue(String.valueOf(hashCode));
        if(balance == null) {
            balance = this.getBalance(hashCode);
            this.memcachedRepository.putInCache(key, glAccountMap);
        }

        return balance;
    }

    public GeneralLedgerAccountBalance getBalance(int hashCode) {
        Query query = new Query();
        query.addCriteria(Criteria.where("code").is(hashCode));
        // Fetch the data
        return this.dataService.findOne(query, this.tenantId, GeneralLedgerAccountBalance.class);
    }

    public Collection<GeneralLedgerAccountBalance> getBalance(int hashCode, int periodId) {
        Query query = new Query();
        Criteria criteria = Criteria.where("subCode").is(hashCode);
        criteria.and("periodId").lt(periodId);
        query.addCriteria(criteria);

        // Fetch the data
        return this.dataService.fetchData(query, this.tenantId, GeneralLedgerAccountBalance.class);
    }

    @Override
    public void loadIntoCache() throws ExecutionException, InterruptedException {

    }


}
