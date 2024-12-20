package com.fyntrac.common.service;

import com.fyntrac.common.cache.collection.CacheMap;
import  com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.entity.factory.InstrumentAttributeFactory;
import com.fyntrac.common.repository.InstrumentAttributeRepository;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class InstrumentAttributeService extends CacheBasedService<InstrumentAttribute> {

    private final InstrumentAttributeRepository instrumentAttributeRepository;
    private final InstrumentAttributeFactory instrumentAttributeFactory;

    @Autowired
    public InstrumentAttributeService(DataService<InstrumentAttribute> dataService
            , MemcachedRepository memcachedRepository
            , InstrumentAttributeRepository instrumentAttributeRepository
            , InstrumentAttributeFactory instrumentAttributeFactory) {
        super(dataService, memcachedRepository);
        this.instrumentAttributeRepository = instrumentAttributeRepository;
        this.instrumentAttributeFactory = instrumentAttributeFactory;
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

    // Define a method in your service class
    public List<InstrumentAttribute> getOpenInstrumentAttributes(String attributeId, String instrumentId, String tenantId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("attributeId").is(attributeId));
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId));
        query.addCriteria(Criteria.where("endDate").is(null));

        return this.dataService.fetchData(query, tenantId, InstrumentAttribute.class);
    }

    // Define a method in your service class
    public InstrumentAttribute getInstrumentAttributeByPeriodId(String tenantId, String attributeId, String instrumentId, int periodId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("attributeId").is(attributeId));
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId));
        query.addCriteria(Criteria.where("periodId").lte(periodId));

        // Sort by versionId in descending order
        query.with(Sort.by(Sort.Order.desc("versionId")));

        // Limit the result to 1
        query.limit(1);

        // Fetch the data
        List<InstrumentAttribute> result = this.dataService.fetchData(query, tenantId, InstrumentAttribute.class);

        // Return the first result if available, otherwise return null
        return result.isEmpty() ? null : result.get(0);
    }

    // Define a method in your service class
    public InstrumentAttribute getInstrumentAttributeByVersionId(String tenantId, long versionId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("versionId").is(versionId));

        // Limit the result to 1
        query.limit(1);

        // Fetch the data
        List<InstrumentAttribute> result = this.dataService.fetchData(query, tenantId, InstrumentAttribute.class);

        // Return the first result if available, otherwise return null
        return result.isEmpty() ? null : result.get(0);
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

    public void addIntoCache(String tenantId, InstrumentAttribute instrumentAttribute) {
        String key = Key.instrumentAttributeList(tenantId);
        CacheMap<InstrumentAttribute> instrumentAttributeCacheMap;
        if(this.memcachedRepository.ifExists(key)) {
            instrumentAttributeCacheMap = this.memcachedRepository.getFromCache(key, CacheMap.class);
          }else{
            instrumentAttributeCacheMap = new CacheMap<InstrumentAttribute>();
         }
        String iaKey = this.getKey(tenantId
                , instrumentAttribute);

        instrumentAttributeCacheMap.put(iaKey,instrumentAttribute);
        this.memcachedRepository.putInCache(key, instrumentAttributeCacheMap);
    }

    private String getKey(String tenantId, InstrumentAttribute instrumentAttribute) {
        return this.getKey(tenantId
                , instrumentAttribute.getAttributeId()
                , instrumentAttribute.getInstrumentId()
                , instrumentAttribute.getPeriodId());
    }

    private String getKey(String tenantId, String attributeId, String instrumentId, int periodId) {
        return Key.instrumentAttributeKey(tenantId
                , attributeId
                , instrumentId
                , periodId);
    }
    public void getInstrumentAttribute(String tenantId, String attributeId, String instrumentId, int periodId) {
        String key = Key.instrumentAttributeList(tenantId);
        CacheMap<InstrumentAttribute> instrumentAttributeCacheMap;
        if(this.memcachedRepository.ifExists(key)) {
            instrumentAttributeCacheMap = this.memcachedRepository.getFromCache(key, CacheMap.class);
        }else{
            instrumentAttributeCacheMap = new CacheMap<InstrumentAttribute>();
        }
        String iaKey = this.getKey(tenantId
                , attributeId, instrumentId, periodId);
         InstrumentAttribute instrumentAttribute =  instrumentAttributeCacheMap.getValue(iaKey);
         if(instrumentAttribute == null) {
             instrumentAttribute = this.getInstrumentAttributeByPeriodId(tenantId, attributeId, instrumentId, periodId);
             if(instrumentAttribute != null) {
                 this.addIntoCache(tenantId, instrumentAttribute);
             }
         }
    }

    public InstrumentAttribute createInstrumentAttribute(String instrumentId,
                                                         String attributeId,
                                                         Date effectiveDate,
                                                         int periodId,
                                                         Map<String, Object> attributes) {
        return instrumentAttributeFactory.create(
                instrumentId,
                attributeId,
                effectiveDate, // effectiveDate
                periodId, // periodId
                new HashMap<>() // attributes
        );
    }
}
