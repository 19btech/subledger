package com.fyntrac.common.service.aggregation;

import com.fyntrac.common.entity.*;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.CacheBasedService;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.utils.Key;
import com.fyntrac.common.key.MetricLevelLtdKey;
import com.fyntrac.common.service.SettingsService;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Metric Level aggregation service
 */
@Service
@Slf4j
public class MetricLevelAggregationService extends CacheBasedService<MetricLevelLtd> {
    private final SettingsService settingsService;
    private final AggregationService aggregationService;
    @Autowired
    public MetricLevelAggregationService(DataService<MetricLevelLtd> dataService
            , SettingsService settingsService
            , MemcachedRepository memcachedRepository
            , AggregationService aggregationService) {
        super(dataService, memcachedRepository);
        this.settingsService = settingsService;
        this.aggregationService = aggregationService;
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
                            memcachedRepository.putInCache(key.getKey(), metricLevelLtd, this.cacheTimeOut);
                        }else {
                            MetricLevelLtd mlLtd1 = MetricLevelLtd.builder()
                                    .accountingPeriodId(accountingPeriod)
                                    .metricName(metricLevelLtd.getMetricName())
                                    .balance(metricLevelLtd.getBalance()).build();
                            memcachedRepository.putInCache(key.getKey(), mlLtd1, this.cacheTimeOut);
                        }
                    }
                }
                pageNumber++;
            }
        }
        this.memcachedRepository.putInCache(Key.allMetricLevelLtdKeyList(tenantId), instrumentList);
    }

    /**
     * Retrieves the balance for a specific instrument, attribute, metric, and accounting period.
     * <p>
     * First, it attempts to fetch the balance from Memcached. If not found, it queries MongoDB,
     * caches the result, and then returns the retrieved balance.
     * </p>
     *
     * @param metric            The metric name.
     * @param accountingPeriodId The accounting period ID.
     * @return An instance of {@link MetricLevelLtd} containing the balance details. Returns {@code null} if not found.
     */
    public MetricLevelLtd getBalance( String metric, int accountingPeriodId) {
        String tenantId = dataService.getTenantId();
        String key = getKey(tenantId, accountingPeriodId, metric);
        log.info("Fetching balance for   Metric: {}, Period: {}, Tenant: {}",
                metric, accountingPeriodId, tenantId);

        try {
            // Check cache first
            if (memcachedRepository.ifExists(key)) {
                log.debug("Cache hit for key: {}", key);
                return memcachedRepository.getFromCache(key, MetricLevelLtd.class);
            } else {
                log.debug("Cache miss for key: {}. Fetching from MongoDB...", key);
            }
        } catch (Exception e) {
            log.error("Error accessing Memcached for key: {}. Proceeding with MongoDB fetch.", key, e);
        }

        // Construct MongoDB query
        Query query = new Query();
        query.addCriteria(Criteria.where("accountingPeriodId").is(accountingPeriodId)
                .and("metricName").is(metric));

        // Fetch from MongoDB
        MetricLevelLtd ltd = null;
        try {
            ltd = dataService.findOne(query, MetricLevelLtd.class);
            if (ltd != null) {
                log.info("Successfully retrieved balance from MongoDB for key: {}", key);
            } else {
                log.warn("No balance found in MongoDB for key: {}", key);
            }
        } catch (DataAccessException e) {
            log.error("MongoDB query failed for  Metric: {}, Period: {}",
                     metric, accountingPeriodId, e);
            return null; // Return null to indicate data retrieval failure
        }

        // Cache the fetched result
        try {
            memcachedRepository.putInCache(key, ltd, cacheTimeOut);
            log.debug("Cached balance for key: {} with timeout: {} seconds", key, cacheTimeOut);
        } catch (Exception e) {
            log.error("Failed to cache data for key: {}", key, e);
        }

        return ltd;
    }

    /**
     * Aggregates transaction activity data by updating or creating balance records.
     * <p>
     * This method calculates activity amount and ending balance for each transaction
     * and updates MongoDB and Memcached accordingly.
     * </p>
     *
     * @param activity The transaction activity to process.
     * @return A list of {@link MetricLevelLtd} records after aggregation.
     */
    public List<MetricLevelLtd> aggregate(TransactionActivity activity) {
        log.info("Starting aggregation for transaction: {}", activity.getTransactionName());

        List<Aggregation> metrics;
        try {
            metrics = this.aggregationService.getMetrics(activity.getTransactionName());
        } catch (Exception e) {
            log.error("Failed to fetch metrics for transaction: {}", activity.getTransactionName(), e);
            return new ArrayList<>();
        }

        List<MetricLevelLtd> aggregates = new ArrayList<>();
        metrics.forEach(aggregation -> {
            boolean currentPeriodLtdFound = true;
            String key = getKey(this.dataService.getTenantId(), activity.getPeriodId(), aggregation.getMetricName());

            MetricLevelLtd ltd;
            try {
                ltd = this.getBalance(aggregation.getMetricName(), activity.getPeriodId());

                if (ltd == null) {
                    log.warn("No balance found for current period. Fetching from previous period.");
                    ltd = this.getBalance(aggregation.getMetricName(), activity.getAccountingPeriod().getPreviousAccountingPeriodId());
                    currentPeriodLtdFound = false;
                }
            } catch (Exception e) {
                log.error("Error retrieving balance for Instrument: {}, Attribute: {}, Metric: {}, Period: {}",
                        activity.getInstrumentId(), activity.getAttributeId(), aggregation.getMetricName(), activity.getPeriodId(), e);
                return;
            }

            double activityAmount = ltd.getBalance().getActivity() + activity.getAmount();
            double endingBalance = activityAmount + ltd.getBalance().getBeginningBalance();

            if (currentPeriodLtdFound) {
                // Update existing balance
                try {
                    ltd.getBalance().setActivity(activityAmount);
                    ltd.getBalance().setEndingBalance(endingBalance);
                    dataService.save(ltd);
                    memcachedRepository.putInCache(key, ltd);
                    log.debug("Updated balance for key: {} with new activity: {} and ending balance: {}", key, activityAmount, endingBalance);
                    aggregates.add(ltd);
                } catch (DataAccessException e) {
                    log.error("Failed to update balance for key: {}", key, e);
                }
            } else {
                // Create a new record for current period
                double beginningBalance = ltd.getBalance().getBeginningBalance();
                activityAmount = activity.getAmount();
                endingBalance = beginningBalance + activityAmount;

                BaseLtd balance = BaseLtd.builder()
                        .endingBalance(endingBalance)
                        .beginningBalance(beginningBalance)
                        .activity(activityAmount)
                        .build();

                MetricLevelLtd metricLevelLtd = MetricLevelLtd.builder()
                        .accountingPeriodId(activity.getPeriodId())
                        .metricName(aggregation.getMetricName())
                        .balance(balance)
                        .build();

                try {
                    dataService.save(metricLevelLtd);
                    memcachedRepository.putInCache(key, metricLevelLtd);
                    log.debug("Created new balance for key: {} with beginning balance: {} and ending balance: {}", key, beginningBalance, endingBalance);
                    aggregates.add(metricLevelLtd);
                } catch (DataAccessException e) {
                    log.error("Failed to save new balance for key: {}", key, e);
                }
            }
        });

        log.info("Aggregation completed for transaction: {}", activity.getTransactionName());
        return aggregates;
    }

    /**
     * Processes multiple transaction activities and aggregates their balances.
     *
     * @param activities A list of transaction activities.
     * @return A list of {@link MetricLevelLtd} records after aggregation.
     */
    public List<MetricLevelLtd> aggregate(Collection<TransactionActivity> activities) {
        log.info("Starting batch aggregation for {} transactions", activities.size());

        List<MetricLevelLtd> aggregates = new ArrayList<>();
        for(TransactionActivity transactionActivity : activities) {
            try {
                aggregates.addAll(this.aggregate(transactionActivity));
            } catch (Exception e) {
                log.error("Error processing transaction: {}", transactionActivity.getTransactionName(), e);
            }
        }

        log.info("Batch aggregation completed for {} transactions", activities.size());
        return aggregates;
    }

    public List<MetricLevelLtd> getBalance(List<String> metrics, int accountingPeriodId) {
        Query query = new Query();
        List<String> metricList = StringUtil.convertUpperCase(metrics);
        // Add criteria to filter by transactionName (list) and transactionDate
        query.addCriteria(Criteria.where("accountingPeriodId").is(accountingPeriodId)
                .and("metricName").in(metrics));

        // Execute the query
        return this.dataService.fetchData(query, MetricLevelLtd.class);
    }

    /**
     * Generates a unique cache key based on tenant ID, accounting period, instrument, attribute, and metric.
     *
     * @param tenantId           The tenant ID.
     * @param accountingPeriodId The accounting period ID.
     * @param metricName         The metric name.
     * @return A unique cache key for the given parameters.
     */
    private String getKey(String tenantId, int accountingPeriodId, String metricName) {
        return String.format("%s-%d-%s", tenantId, accountingPeriodId, metricName);
    }
}
