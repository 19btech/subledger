package com.fyntrac.common.service.aggregation;

import com.fyntrac.common.entity.*;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.CacheBasedService;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.utils.Key;
import com.fyntrac.common.key.InstrumentLevelLtdKey;
import com.fyntrac.common.service.SettingsService;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.Future;

@Service
@Slf4j
public class InstrumentLevelAggregationService extends CacheBasedService<InstrumentLevelLtd> {
    private final SettingsService settingsService;
    private final AggregationService aggregationService;
    @Autowired
    public InstrumentLevelAggregationService(DataService<InstrumentLevelLtd> dataService
                                            , SettingsService settingsService
                                            , MemcachedRepository memcachedRepository
                                            , AggregationService aggregationService) {
        super(dataService, memcachedRepository);
        this.settingsService = settingsService;
        this.aggregationService = aggregationService;
    }


    /**
     * Persist data
     * @param instrumentLevelLtd
     */
    @Override
    public InstrumentLevelLtd save(InstrumentLevelLtd instrumentLevelLtd) {
        this.dataService.save(instrumentLevelLtd);

        String key = getKey(this.dataService.getTenantId()
                , instrumentLevelLtd.getPostingDate()
                , instrumentLevelLtd.getInstrumentId()
                , instrumentLevelLtd.getMetricName());

        this.memcachedRepository.putInCache(key, instrumentLevelLtd);
        return instrumentLevelLtd;
    }

    public Collection<InstrumentLevelLtd> save(List<InstrumentLevelLtd> balances) {
        Collection<InstrumentLevelLtd> ltdBalances = this.dataService.saveAll(balances, InstrumentLevelLtd.class);

        for(InstrumentLevelLtd instrumentLevelLtd : ltdBalances) {
            String key = getKey(this.dataService.getTenantId()
                    , instrumentLevelLtd.getPostingDate()
                    , instrumentLevelLtd.getInstrumentId()
                    , instrumentLevelLtd.getMetricName());
            this.memcachedRepository.putInCache(key, instrumentLevelLtd);
        }

        return ltdBalances;
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
    public void loadIntoCache() {}

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
                            memcachedRepository.putInCache(key.getKey(), instrumentLevelLtd, this.cacheTimeOut);
                        }else {
                            memcachedRepository.putInCache(key.getKey(), instrumentLevelLtd, this.cacheTimeOut);
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

    /**
     * Retrieves the balance for a specific instrument, attribute, metric, and accounting period.
     * <p>
     * First, it attempts to fetch the balance from Memcached. If not found, it queries MongoDB,
     * caches the result, and then returns the retrieved balance.
     * </p>
     *
     * @param instrumentId      The ID of the instrument.
     * @param metric            The metric name.
     * @param postingDate The accounting posting date.
     * @return An instance of {@link InstrumentLevelLtd} containing the balance details. Returns {@code null} if not found.
     */
    public InstrumentLevelLtd getBalance(String instrumentId, String metric, int postingDate) {
        String tenantId = dataService.getTenantId();
        return this.getBalance(tenantId, instrumentId, metric, postingDate);
    }

    /**
     * Retrieves the balance for a specific instrument, attribute, metric, and accounting period.
     * <p>
     * First, it attempts to fetch the balance from Memcached. If not found, it queries MongoDB,
     * caches the result, and then returns the retrieved balance.
     * </p>
     *
     * @param tenantId          Tenant ID
     * @param instrumentId      The ID of the instrument.
     * @param metric            The metric name.
     * @param postingDate The accounting posting date.
     * @return An instance of {@link InstrumentLevelLtd} containing the balance details. Returns {@code null} if not found.
     */
    public InstrumentLevelLtd getBalance(String tenantId, String instrumentId, String metric, int postingDate) {
        InstrumentLevelLtdKey key = new InstrumentLevelLtdKey(tenantId, metric, instrumentId, postingDate);
        log.info("Fetching balance for Instrument: {},  Metric: {}, Period: {}, Tenant: {}",
                instrumentId, metric, postingDate, tenantId);

        try {
            // Check cache first
            if (memcachedRepository.ifExists(key.getKey())) {
                log.debug("Cache hit for key: {}", key);
                return memcachedRepository.getFromCache(key.getKey(), InstrumentLevelLtd.class);
            } else {
                log.debug("Cache miss for key: {}. Fetching from MongoDB...", key);
            }
        } catch (Exception e) {
            log.error("Error accessing Memcached for key: {}. Proceeding with MongoDB fetch.", key, e);
        }

        // Construct MongoDB query
        Query query = new Query();
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId)
                .and("postingDate").is(postingDate)
                .and("metricName").is(metric));

        // Fetch from MongoDB
        InstrumentLevelLtd ltd = null;
        try {
            ltd = dataService.findOne(query, tenantId, InstrumentLevelLtd.class);
            if (ltd != null) {
                log.info("Successfully retrieved balance from MongoDB for key: {}", key);
            } else {
                log.warn("No balance found in MongoDB for key: {}", key);
            }
        } catch (DataAccessException e) {
            log.error("MongoDB query failed for Instrument: {}, Metric: {}, Period: {}",
                    instrumentId, metric, postingDate, e);
            return null; // Return null to indicate data retrieval failure
        }

        if(ltd != null) {
            // Cache the fetched result
            try {
                memcachedRepository.putInCache(key.getKey(), ltd, cacheTimeOut);
                log.debug("Cached balance for key: {} with timeout: {} seconds", key, cacheTimeOut);
            } catch (Exception e) {
                log.error("Failed to cache data for key: {}", key, e);
            }
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
     * @return A list of {@link InstrumentLevelLtd} records after aggregation.
     */
    public Collection<InstrumentLevelLtd> aggregate(TransactionActivity activity, int postingDate) {
        log.info("Starting aggregation for transaction: {}", activity.getTransactionName());

        List<Aggregation> metrics;
        try {
            metrics = this.aggregationService.getMetrics(activity.getTransactionName().toUpperCase());
        } catch (Exception e) {
            log.error("Failed to fetch metrics for transaction: {}", activity.getTransactionName(), e);
            return new ArrayList<>();
        }

        List<InstrumentLevelLtd> aggregates = new ArrayList<>();
        for (Aggregation aggregation : metrics) {
            boolean currentPeriodLtdFound = true;
            String key = getKey(this.dataService.getTenantId(), activity.getPeriodId(), activity.getInstrumentId(), aggregation.getMetricName());

            InstrumentLevelLtd ltd;
            try {
                ltd = this.getBalance(activity.getInstrumentId(), aggregation.getMetricName(), activity.getPostingDate());

                if (ltd == null) {
                    log.warn("No balance found for current period. Fetching from previous period.");
                    ltd = this.getBalance(activity.getInstrumentId(), aggregation.getMetricName(), postingDate);
                    currentPeriodLtdFound = false;
                }
            } catch (Exception e) {
                log.error("Error retrieving balance for Instrument: {}, Attribute: {}, Metric: {}, Period: {}",
                        activity.getInstrumentId(), activity.getAttributeId(), aggregation.getMetricName(), activity.getPeriodId(), e);
                continue; // Use continue to skip to the next iteration
            }

            BigDecimal activityAmount = ltd.getBalance().getActivity().add(activity.getAmount());
            BigDecimal endingBalance = ltd.getBalance().getBeginningBalance().add(activityAmount);

            if (currentPeriodLtdFound) {
                // Update existing balance
                try {
                    ltd.getBalance().setActivity(activityAmount);
                    ltd.getBalance().setEndingBalance(endingBalance);
                    log.debug("Updated balance for key: {} with new activity: {} and ending balance: {}", key, activityAmount, endingBalance);
                    aggregates.add(ltd);
                } catch (DataAccessException e) {
                    log.error("Failed to update balance for key: {}", key, e);
                }
            } else {
                // Create a new record for current period
                BigDecimal beginningBalance = ltd.getBalance().getEndingBalance();
                activityAmount = activity.getAmount();
                endingBalance = beginningBalance.add(activityAmount);

                BaseLtd balance = BaseLtd.builder()
                        .endingBalance(endingBalance)
                        .beginningBalance(beginningBalance)
                        .activity(activityAmount)
                        .build();

                InstrumentLevelLtd instrumentLevelLtd = InstrumentLevelLtd.builder()
                        .accountingPeriodId(activity.getPeriodId())
                        .instrumentId(activity.getInstrumentId())
                        .metricName(aggregation.getMetricName())
                        .postingDate(activity.getPostingDate())
                        .balance(balance)
                        .build();

                try {
                    log.debug("Created new balance for key: {} with beginning balance: {} and ending balance: {}", key, beginningBalance, endingBalance);
                    aggregates.add(instrumentLevelLtd);
                } catch (DataAccessException e) {
                    log.error("Failed to save new balance for key: {}", key, e);
                }
            }
        }

        log.info("Aggregation completed for transaction: {}", activity.getTransactionName());
        return this.save(aggregates);

    }

    /**
     * Processes multiple transaction activities and aggregates their balances.
     *
     * @param activities A list of transaction activities.
     * @return A list of {@link InstrumentLevelLtd} records after aggregation.
     */
    public List<InstrumentLevelLtd> aggregate(Collection<TransactionActivity> activities, int previousPostingDate) {
        log.info("Starting batch aggregation for {} transactions", activities.size());

        List<InstrumentLevelLtd> aggregates = new ArrayList<>();
        for(TransactionActivity transactionActivity : activities) {
            try {
                aggregates.addAll(this.aggregate(transactionActivity, previousPostingDate));
            } catch (Exception e) {
                log.error("Error processing transaction: {}", transactionActivity.getTransactionName(), e);
            }
        }

        log.info("Batch aggregation completed for {} transactions", activities.size());
        return aggregates;
    }

    public List<InstrumentLevelLtd> getBalance(String instrumentId, List<String> metrics, int postingDate) {
        Query query = new Query();
        List<String> metricList = StringUtil.convertUpperCase(metrics);
        // Add criteria to filter by transactionName (list) and transactionDate
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId.toUpperCase())
                .and("postingDate").is(postingDate)
                .and("metricName").in(metricList));

        // Execute the query
        return this.dataService.fetchData(query, InstrumentLevelLtd.class);
    }
    /**
     * Generates a unique cache key based on tenant ID, accounting period, instrument, attribute, and metric.
     *
     * @param tenantId           The tenant ID.
     * @param accountingPeriodId The accounting period ID.
     * @param instrumentId       The instrument ID.
     * @param metricName         The metric name.
     * @return A unique cache key for the given parameters.
     */

    private String getKey(String tenantId, int accountingPeriodId, String instrumentId, String metricName) {
        String key = String.format("%s-%d-%s-%s",
                tenantId,
                accountingPeriodId,
                instrumentId.toUpperCase(),
                metricName.toUpperCase());
        String cleanedKey = StringUtil.removeSpaces(key);
        return StringUtil.generateSHA256Hash(cleanedKey);
    }
}

