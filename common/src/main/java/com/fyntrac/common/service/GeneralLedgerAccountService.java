package com.fyntrac.common.service;


import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.entity.GeneralLedgerAccountBalanceStage;
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
public class GeneralLedgerAccountService extends CacheBasedService<GeneralLedgerAccountBalanceStage> {

    private String tenantId;

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
        this.dataService.setTenantId(tenantId);
    }
    @Autowired
    public GeneralLedgerAccountService(DataService<GeneralLedgerAccountBalanceStage> dataService
            , MemcachedRepository memcachedRepository){
        super(dataService, memcachedRepository);
    }

    @Override
    public GeneralLedgerAccountBalanceStage save(GeneralLedgerAccountBalanceStage generalLedgerAccountBalanceStage) {
        if(this.tenantId == null) {
            this.dataService.getTenantId();
        }

        return this.dataService.save(generalLedgerAccountBalanceStage, tenantId);
    }

    public void saveAll(Set<GeneralLedgerAccountBalanceStage> gleAccountsBalance) {
        this.dataService.saveAll(gleAccountsBalance, this.tenantId, GeneralLedgerAccountBalanceStage.class);
    }

    @Override
    public Collection<GeneralLedgerAccountBalanceStage> fetchAll() {
        return this.dataService.fetchAllData(GeneralLedgerAccountBalanceStage.class);
    }

    public GeneralLedgerAccountBalanceStage getGeneralLedgerAccountBalance(int hashCode) {
        String key = Key.generalLedgerAccountTypesStageKey(this.tenantId);
        CacheMap<GeneralLedgerAccountBalanceStage> glAccountMap;
        if(!(this.memcachedRepository.ifExists(key))) {
            glAccountMap = new CacheMap<>();
            GeneralLedgerAccountBalanceStage generalLedgerAccountBalanceStage =  this.getBalance(hashCode);
            glAccountMap.put(String.valueOf(hashCode), generalLedgerAccountBalanceStage);
            this.memcachedRepository.putInCache(key, glAccountMap);
        }
        glAccountMap = this.memcachedRepository.getFromCache(key, CacheMap.class);
        return glAccountMap.getValue(String.valueOf(hashCode));
    }

    public GeneralLedgerAccountBalanceStage getBalance(int hashCode) {
        Query query = new Query();
        query.addCriteria(Criteria.where("hashCode").is(hashCode));
        // Fetch the data
        return this.dataService.findOne(query, this.tenantId, GeneralLedgerAccountBalanceStage.class);
    }
    @Override
    public void loadIntoCache() throws ExecutionException, InterruptedException {

    }


}
