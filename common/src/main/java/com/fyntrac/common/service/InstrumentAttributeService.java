package com.fyntrac.common.service;

import  com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.repository.InstrumentAttributeRepository;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class InstrumentAttributeService extends CacheBasedService<InstrumentAttribute> {

    private InstrumentAttributeRepository instrumentAttributeRepository;

    @Autowired
    public InstrumentAttributeService(DataService<InstrumentAttribute> dataService
            , MemcachedRepository memcachedRepository, InstrumentAttributeRepository instrumentAttributeRepository) {
        super(dataService, memcachedRepository);
        this.instrumentAttributeRepository = instrumentAttributeRepository;
    }

    public List<InstrumentAttribute> getLastOpenInstrumentAttributes(String attributeId, String instrumentId) {
        return instrumentAttributeRepository.findByAttributeIdAndInstrumentIdAndEndDateIsNull(attributeId, instrumentId);
    }

    // Define a method in your service class
    public List<InstrumentAttribute> getOpenInstrumentAttributes(String attributeId, String instrumentId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("attributeId").is(attributeId));
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId));
        query.addCriteria(Criteria.where("endDate").is(null));

        return this.dataService.fetchData(query, InstrumentAttribute.class);
    }

    public List<InstrumentAttribute> findByAttributeIdAndInstrumentId(String attributeId, String instrumentId) {
        return instrumentAttributeRepository.findByAttributeIdAndInstrumentId(attributeId, instrumentId);
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
    public void loadIntoCache() throws ExecutionException, InterruptedException {
        ReferenceData referenceData = this.memcachedRepository.getFromCache(this.dataService.getTenantId(), ReferenceData.class);
        if(referenceData != null) {
            int previousAccountingPeriodId = referenceData.getPrevioudAccountingPeriodId();
            this.loadIntoCache(previousAccountingPeriodId);
            int currentAccountingPeriodId = referenceData.getCurrentAccountingPeriodId();
            this.loadIntoCache(currentAccountingPeriodId);
        }
    }

    public void loadIntoCache(int accountingPeriodId) throws ExecutionException, InterruptedException {
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
