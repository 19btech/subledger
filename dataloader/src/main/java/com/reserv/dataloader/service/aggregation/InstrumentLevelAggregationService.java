package com.reserv.dataloader.service.aggregation;

import com.reserv.dataloader.config.ReferenceData;
import com.reserv.dataloader.entity.InstrumentLevelLtd;
import com.reserv.dataloader.entity.Settings;
import com.reserv.dataloader.key.InstrumentLevelLtdKey;
import com.reserv.dataloader.repository.MemcachedRepository;
import com.reserv.dataloader.service.CacheBasedService;
import com.reserv.dataloader.service.DataService;
import com.reserv.dataloader.service.SettingsService;
import com.reserv.dataloader.utils.Key;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
            query.addCriteria(Criteria.where("periodId").gte(accountingPeriod));
            query.with(Sort.by(Sort.Direction.DESC, "periodId"));
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
                            InstrumentLevelLtd mlLtd1 = InstrumentLevelLtd.builder()
                                    .accountingPeriodId(accountingPeriod)
                                    .metricName(instrumentLevelLtd.getMetricName())
                                    .balance(instrumentLevelLtd.getBalance()).build();
                            memcachedRepository.putInCache(key.getKey(), mlLtd1, 3456000);
                        }
                    }
                }
                pageNumber++;
            }
        }

        this.memcachedRepository.putInCache(Key.allInstrumentLevelLtdKeyList(tenantId), instrumentList);
    }

}

