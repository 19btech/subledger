package com.reserv.dataloader.service;

import com.reserv.dataloader.config.ReferenceData;
import com.reserv.dataloader.entity.TransactionActivity;
import com.reserv.dataloader.repository.MemcachedRepository;
import com.reserv.dataloader.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class TransactionActivityService extends CacheBasedService<TransactionActivity> {
    @Autowired
    public TransactionActivityService(DataService<TransactionActivity> dataService
                                      , MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
    }

    @Override
    public void save(TransactionActivity activity) {
        this.dataService.save(activity);
    }


    @Override
    public Collection<TransactionActivity> fetchAll() {
        return dataService.fetchAllData(TransactionActivity.class);
    }

    @Override
    public void loadIntoCache() {
        ReferenceData referenceData = this.memcachedRepository.getFromCache(this.dataService.getTenantId(), ReferenceData.class);
        int previousAccountingPeriodId = 0;
        if(referenceData != null) {
            previousAccountingPeriodId = referenceData.getPrevioudAccountingPeriodId();

            Set<String> instrumentIds = this.getInstrumentIdsByPeriodId(previousAccountingPeriodId);
            String key = Key.previoudPeriodInstrumentsKey(this.dataService.getTenantId());
            boolean ifExists = this.memcachedRepository.ifExists(key);

            if(ifExists) {
                this.memcachedRepository.delete(key);
             }
            this.memcachedRepository.putInCache(key, instrumentIds);
        }
    }

    public Set<String> getInstrumentIdsByPeriodId(int periodId) {
        Query query = new Query(Criteria.where("periodId").is(periodId));
        List<TransactionActivity> activities = this.dataService.fetchData(query, TransactionActivity.class);
        List<String> instrumentIds = new ArrayList<>();
        for (TransactionActivity ta : activities) {
            instrumentIds.add(ta.getInstrumentId());
        }
        return new HashSet<>(instrumentIds);
    }

}
