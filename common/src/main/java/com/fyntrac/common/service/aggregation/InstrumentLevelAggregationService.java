package com.fyntrac.common.service.aggregation;

import com.fyntrac.common.entity.AttributeLevelLtd;
import com.fyntrac.common.entity.InstrumentLevelLtd;
import com.fyntrac.common.entity.Settings;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.CacheBasedService;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.utils.Key;
import com.fyntrac.common.key.InstrumentLevelLtdKey;
import com.fyntrac.common.service.SettingsService;
import com.fyntrac.common.utils.StringUtil;
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
public class InstrumentLevelAggregationService extends CacheBasedService<InstrumentLevelLtd> {
    private final SettingsService settingsService;
    @Autowired
    public InstrumentLevelAggregationService(DataService<InstrumentLevelLtd> dataService
                                             ,SettingsService settingsService
                                            , MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
        this.settingsService = settingsService;
    }


    /**
     * Persist data
     * @param instrumentLevelLtd
     */
    @Override
    public void save(InstrumentLevelLtd instrumentLevelLtd) {
        this.dataService.save(instrumentLevelLtd);
        String key = this.dataService.getTenantId() + instrumentLevelLtd.hashCode();
        this.memcachedRepository.putInCache(key, instrumentLevelLtd);

    }

    /**
     * fetch all data
     * @return
     */
    @Override
    public List<InstrumentLevelLtd> fetchAll() {
        return dataService.fetchAllData(InstrumentLevelLtd.class);
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
            List<InstrumentLevelLtd> chunk = dataService.fetchData(query, InstrumentLevelLtd.class);
            if (chunk.isEmpty()) {
                hasMore = false;
            } else {
                for (InstrumentLevelLtd instrumentLevelLtd : chunk) {
                    InstrumentLevelLtdKey key = new InstrumentLevelLtdKey(tenantId
                            , instrumentLevelLtd.getMetricName()
                            , instrumentLevelLtd.getInstrumentId()
                            , accountingPeriod);


                    if(!memcachedRepository.ifExists(key.getKey())) {
                        if(instrumentLevelLtd.getAccountingPeriodId() == accountingPeriod) {
                            memcachedRepository.putInCache(key.getKey(), instrumentLevelLtd, 3456000);
                        }else {
                            memcachedRepository.putInCache(key.getKey(), instrumentLevelLtd, 3456000);
                        }
                    }
                    instrumentList.add(key.getKey());
                }
                pageNumber++;
            }
        }
        if(this.memcachedRepository.ifExists(Key.allInstrumentLevelLtdKeyList(tenantId))) {
            Future<Boolean> future = this.memcachedRepository.putInCache(Key.allInstrumentLevelLtdKeyList(tenantId), instrumentList);
        }else {
            Future<Boolean> future = this.memcachedRepository.putInCache(Key.allInstrumentLevelLtdKeyList(tenantId), instrumentList);
        }
    }

    public List<InstrumentLevelLtd> getBalance(String instrumentId, List<String> metrics, int accountingPeriodId) {
        Query query = new Query();
        List<String> metricList = StringUtil.convertUpperCase(metrics);
        // Add criteria to filter by transactionName (list) and transactionDate
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId)
                .and("accountingPeriodId").is(accountingPeriodId)
                .and("metricName").in(metricList));

        // Execute the query
        return this.dataService.fetchData(query, InstrumentLevelLtd.class);
    }
}

