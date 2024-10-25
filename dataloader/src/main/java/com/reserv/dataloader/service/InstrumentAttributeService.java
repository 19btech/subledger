package com.reserv.dataloader.service;

import  com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.reserv.dataloader.repository.MemcachedRepository;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class InstrumentAttributeService extends CacheBasedService<InstrumentAttribute> {

    @Autowired
    public InstrumentAttributeService(DataService<InstrumentAttribute> dataService
                                      , MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
    }

    @Override
    public void save(InstrumentAttribute ia) {
        this.dataService.save(ia);
    }

    @Override
    public Collection<InstrumentAttribute> fetchAll() {
        return  this.dataService.fetchAllData(InstrumentAttribute.class);
    }

    @Override
    public void loadIntoCache() {
        ReferenceData referenceData = this.memcachedRepository.getFromCache(this.dataService.getTenantId(), ReferenceData.class);
        if(referenceData != null) {
            int previousAccountingPeriodId = referenceData.getPrevioudAccountingPeriodId();
            this.loadIntoCache(previousAccountingPeriodId);
            int currentAccountingPeriodId = referenceData.getCurrentAccountingPeriodId();
            this.loadIntoCache(currentAccountingPeriodId);
        }
    }

    public void loadIntoCache(int accountingPeriodId) {
        Set<String> instrumentIds = this.getInstrumentIdsByPeriodId(accountingPeriodId);
        String key = Key.previoudPeriodInstrumentsKey(this.dataService.getTenantId());
        boolean ifExists = this.memcachedRepository.ifExists(key);

        if(ifExists) {
            this.memcachedRepository.delete(key);
        }
        this.memcachedRepository.putInCache(key, instrumentIds);
    }
    /**
     * to get all active instruments
     * @param periodId
     * @return
     */
    public Set<String> getInstrumentIdsByPeriodId(int periodId) {
        Query query = new Query(Criteria.where("periodId").is(periodId));
        List<InstrumentAttribute> activities = this.dataService.fetchData(query, InstrumentAttribute.class);
        List<String> instrumentIds = new ArrayList<>();
        for (InstrumentAttribute ia : activities) {
            instrumentIds.add(ia.getInstrumentId());
        }
        return new HashSet<>(instrumentIds);
    }
}
