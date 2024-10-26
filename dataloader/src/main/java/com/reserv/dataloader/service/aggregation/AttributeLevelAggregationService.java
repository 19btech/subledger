package com.reserv.dataloader.service.aggregation;

import  com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.entity.AttributeLevelLtd;
import com.fyntrac.common.entity.Settings;
import com.reserv.dataloader.key.AttributeLevelLtdKey;
import com.fyntrac.common.repository.MemcachedRepository;
import com.reserv.dataloader.service.CacheBasedService;
import com.reserv.dataloader.service.DataService;
import com.reserv.dataloader.service.SettingsService;
import com.fyntrac.common.utils.Key;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

@Service
public class AttributeLevelAggregationService extends CacheBasedService<AttributeLevelLtd> {
    private final SettingsService settingsService;
    @Autowired
    public AttributeLevelAggregationService(DataService<AttributeLevelLtd> dataService, SettingsService settingsService, MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
        this.settingsService = settingsService;
    }


    /**
     * Persist data
     * @param attributeLevelLtd
     */
    @Override
    public void save(AttributeLevelLtd attributeLevelLtd) {
        this.dataService.save(attributeLevelLtd);
        String key = this.dataService.getTenantId() + attributeLevelLtd.hashCode();
        this.memcachedRepository.putInCache(key, attributeLevelLtd);

    }

    /**
     * fetch all data
     * @return
     */
    @Override
    public List<AttributeLevelLtd> fetchAll() {
        return dataService.fetchAllData(AttributeLevelLtd.class);
    }

    /**
     * load data into cache
     */
    @Override
    public void loadIntoCache() {
        Settings s = this.settingsService.fetch();
        int lastActivityUploadAccountingPeriod = s.getLastTransactionActivityUploadReportingPeriod();
        this.loadIntoCache(lastActivityUploadAccountingPeriod, this.dataService.getTenantId());
    }

    /**
     * to load data for an accounting period
     * @param accountingPeriod
     * @param tenantId
     */
    public void loadIntoCache(int accountingPeriod, String tenantId) {
        int chunkSize = 10000;
        int pageNumber = 0;
        boolean hasMore = true;
        Set<String> instrumentList = new HashSet<>();
        while (hasMore) {
            Query query = new Query().limit(chunkSize).skip(pageNumber * chunkSize);
            query.addCriteria(Criteria.where("accountingPeriodId").gte(accountingPeriod));
            query.with(Sort.by(Sort.Direction.DESC, "accountingPeriodId"));
            List<AttributeLevelLtd> chunk = dataService.fetchData(query, AttributeLevelLtd.class);
            if (chunk.isEmpty()) {
                hasMore = false;
            } else {
                for (AttributeLevelLtd attributeLevelLtd : chunk) {
                    AttributeLevelLtdKey key = new AttributeLevelLtdKey(tenantId
                            , attributeLevelLtd.getMetricName()
                            , attributeLevelLtd.getInstrumentId()
                            , attributeLevelLtd.getAttributeId()
                            , accountingPeriod);


                    if(!memcachedRepository.ifExists(key.getKey())) {
                        if(attributeLevelLtd.getAccountingPeriodId() == accountingPeriod) {
                            memcachedRepository.putInCache(key.getKey(), attributeLevelLtd, 3456000);
                        }else {
                            memcachedRepository.putInCache(key.getKey(), attributeLevelLtd, 3456000);
                        }
                    }
                    instrumentList.add(key.getKey());
                }
                pageNumber++;
            }
        }
        if(this.memcachedRepository.ifExists(Key.allAttributeLevelLtdKeyList(tenantId))) {
            Future<Boolean> future = this.memcachedRepository.putInCache(Key.allAttributeLevelLtdKeyList(tenantId), instrumentList);
        }else {
            Future<Boolean> future = this.memcachedRepository.putInCache(Key.allAttributeLevelLtdKeyList(tenantId), instrumentList);
        }
    }

}

