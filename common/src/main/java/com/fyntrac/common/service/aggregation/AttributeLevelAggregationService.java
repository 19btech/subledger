package com.fyntrac.common.service.aggregation;

import com.fyntrac.common.entity.AttributeLevelLtd;
import com.fyntrac.common.entity.Settings;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.CacheBasedService;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.utils.Key;
import com.fyntrac.common.key.AttributeLevelLtdKey;
import com.fyntrac.common.service.SettingsService;
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

    public List<AttributeLevelLtd> getBalance(String instrumentId, String attributeId, List<String> metrics, int accountingPeriodId) {
        Query query = new Query();

        // Add criteria to filter by transactionName (list) and transactionDate
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId)
                .and("attributeId").is(attributeId)
                .and("accountingPeriodId").is(accountingPeriodId)
                .and("metricName").in(metrics));

        // Execute the query
        return this.dataService.fetchData(query, AttributeLevelLtd.class);
    }
}

