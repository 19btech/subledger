package com.reserv.dataloader.service.aggregation;

import com.fyntrac.common.entity.MetricLevelLtd;
import com.fyntrac.common.entity.Settings;
import com.reserv.dataloader.key.MetricLevelLtdKey;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.CacheBasedService;
import com.fyntrac.common.service.DataService;
import com.reserv.dataloader.service.SettingsService;
import com.fyntrac.common.utils.Key;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Metric Level aggregation service
 */
@Service
public class MetricLevelAggregationService extends CacheBasedService<MetricLevelLtd> {
    private final SettingsService settingsService;
    @Autowired
    public MetricLevelAggregationService(DataService<MetricLevelLtd> dataService
            , SettingsService settingsService
            , MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
        this.settingsService = settingsService;
    }


    /**
     * persists object
     * @param metricLevelLtd
     */
    @Override
    public void save(MetricLevelLtd metricLevelLtd) {
        this.dataService.save(metricLevelLtd);
        String key = this.dataService.getTenantId() + metricLevelLtd.hashCode();
        this.memcachedRepository.putInCache(key, metricLevelLtd);

    }

    /**
     * get all data
     * @return
     */
    @Override
    public List<MetricLevelLtd> fetchAll() {
        return dataService.fetchAllData(MetricLevelLtd.class);
    }

    /**
     * Load data into cache
     */
    @Override
    public void loadIntoCache() {
        Settings s = this.settingsService.fetch();
        int lastActivityUploadAccountingPeriod = s.getLastTransactionActivityUploadReportingPeriod();
        this.loadIntoCache(lastActivityUploadAccountingPeriod, this.dataService.getTenantId());
    }

    /**
     * Load data into cache
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
            List<MetricLevelLtd> chunk = dataService.fetchData(query, MetricLevelLtd.class);
            if (chunk.isEmpty()) {
                hasMore = false;
            } else {
                for (MetricLevelLtd metricLevelLtd : chunk) {
                    MetricLevelLtdKey key = new MetricLevelLtdKey(tenantId, metricLevelLtd.getMetricName(), accountingPeriod);


                    if(!memcachedRepository.ifExists(key.getKey())) {
                        if(metricLevelLtd.getAccountingPeriodId() == accountingPeriod) {
                            memcachedRepository.putInCache(key.getKey(), metricLevelLtd, 3456000);
                        }else {
                            MetricLevelLtd mlLtd1 = MetricLevelLtd.builder()
                                    .accountingPeriodId(accountingPeriod)
                                    .metricName(metricLevelLtd.getMetricName())
                                    .balance(metricLevelLtd.getBalance()).build();
                            memcachedRepository.putInCache(key.getKey(), mlLtd1, 3456000);
                        }
                    }
                }
                pageNumber++;
            }
        }
        this.memcachedRepository.putInCache(Key.allMetricLevelLtdKeyList(tenantId), instrumentList);
    }

}
