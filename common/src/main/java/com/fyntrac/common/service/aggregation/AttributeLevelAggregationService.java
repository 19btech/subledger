package com.fyntrac.common.service.aggregation;

import com.fyntrac.common.entity.*;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.CacheBasedService;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.utils.Key;
import com.fyntrac.common.key.AttributeLevelLtdKey;
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
public class AttributeLevelAggregationService extends CacheBasedService<AttributeLevelLtd> {
    private final SettingsService settingsService;
    private final AggregationService aggregationService;

    /**
     * Constructor for AggregationService.
     *
     * @param dataService          The data service handling MongoDB operations.
     * @param settingsService      Configuration service
     * @param memcachedRepository  The caching repository using Memcached.
     * @param aggregationService   The service handling metric aggregation.
     *
     */
    @Autowired
    public AttributeLevelAggregationService(DataService<AttributeLevelLtd> dataService
            , SettingsService settingsService
            , MemcachedRepository memcachedRepository
            , AggregationService aggregationService) {
        super(dataService, memcachedRepository);
        this.settingsService = settingsService;
        this.aggregationService = aggregationService;
    }


    /**
     * Persist data
     * @param attributeLevelLtd
     */
    @Override
    public AttributeLevelLtd save(AttributeLevelLtd attributeLevelLtd) {
        this.dataService.save(attributeLevelLtd);
        String key = getKey(this.dataService.getTenantId()
                , attributeLevelLtd.getPostingDate()
                , attributeLevelLtd.getInstrumentId()
                , attributeLevelLtd.getAttributeId()
                , attributeLevelLtd.getMetricName());

        this.memcachedRepository.putInCache(key, attributeLevelLtd);
        return attributeLevelLtd;
    }

    public Collection<AttributeLevelLtd> save(List<AttributeLevelLtd> balances) {
        Collection<AttributeLevelLtd> ltdBalances = this.dataService.saveAll(balances, AttributeLevelLtd.class);

        for(AttributeLevelLtd attributeLevelLtd : ltdBalances) {
            String key = getKey(this.dataService.getTenantId()
                    , attributeLevelLtd.getPostingDate()
                    , attributeLevelLtd.getInstrumentId()
                    , attributeLevelLtd.getAttributeId()
                    , attributeLevelLtd.getMetricName());
            this.memcachedRepository.putInCache(key, attributeLevelLtd);
        }
        return ltdBalances;
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

    }

    /**
     * to load data for an accounting period
     * @param postingDate
     * @param tenantId
     */
    public void loadIntoCache(int postingDate, String tenantId) {
        int chunkSize = 10000;
        int pageNumber = 0;
        boolean hasMore = true;
        Set<String> instrumentList = new HashSet<>();
        while (hasMore) {
            Query query = new Query().limit(chunkSize).skip(pageNumber * chunkSize);
            query.addCriteria(Criteria.where("postingDate").gte(postingDate));
            query.with(Sort.by(Sort.Direction.DESC, "postingDate"));
            List<AttributeLevelLtd> chunk = dataService.fetchData(query, AttributeLevelLtd.class);
            if (chunk.isEmpty()) {
                hasMore = false;
            } else {
                for (AttributeLevelLtd attributeLevelLtd : chunk) {
                    AttributeLevelLtdKey key = new AttributeLevelLtdKey(tenantId
                            , attributeLevelLtd.getMetricName()
                            , attributeLevelLtd.getInstrumentId()
                            , attributeLevelLtd.getAttributeId()
                            , postingDate);


                    if(!memcachedRepository.ifExists(key.getKey())) {
                        if(attributeLevelLtd.getPostingDate() == postingDate) {
                            memcachedRepository.putInCache(key.getKey(), attributeLevelLtd, this.cacheTimeOut);
                        }else {
                            memcachedRepository.putInCache(key.getKey(), attributeLevelLtd, this.cacheTimeOut);
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

    /**
     * Retrieves the balance for a specific instrument, attribute, metric, and accounting period.
     * <p>
     * First, it attempts to fetch the balance from Memcached. If not found, it queries MongoDB,
     * caches the result, and then returns the retrieved balance.
     * </p>
     *
     * @param instrumentId      The ID of the instrument.
     * @param attributeId       The ID of the attribute.
     * @param metric            The metric name.
     * @param postingDate Posting Date.
     * @return An instance of {@link AttributeLevelLtd} containing the balance details. Returns {@code null} if not found.
     */
    public AttributeLevelLtd getBalance(String instrumentId, String attributeId, String metric, int postingDate) {
        String tenantId = dataService.getTenantId();
        return this.getBalance(tenantId, instrumentId, attributeId, metric, postingDate);
    }

    /**
     * Retrieves the balance for a specific tenant instrument, attribute, metric, and accounting period.
     * <p>
     * First, it attempts to fetch the balance from Memcached. If not found, it queries MongoDB,
     * caches the result, and then returns the retrieved balance.
     * </p>
     *
     * @param tenantId          Tenant ID
     * @param instrumentId      The ID of the instrument.
     * @param attributeId       The ID of the attribute.
     * @param metric            The metric name.
     * @param postingDate Posting Date.
     * @return An instance of {@link AttributeLevelLtd} containing the balance details. Returns {@code null} if not found.
     */
    public AttributeLevelLtd getBalance(String tenantId, String instrumentId, String attributeId, String metric, int postingDate) {
        AttributeLevelLtdKey key = new AttributeLevelLtdKey(tenantId, metric.toUpperCase(), instrumentId, attributeId, postingDate);
        log.info("Fetching balance for Instrument: {}, Attribute: {}, Metric: {}, PostingDate: {}, Tenant: {}",
                instrumentId, attributeId, metric, postingDate, tenantId);
//        try {
//            // Check cache first
//            if (memcachedRepository.ifExists(key.getKey())) {
//                log.debug("Cache hit for key: {}", key);
//                return memcachedRepository.getFromCache(key.getKey(), AttributeLevelLtd.class);
//            } else {
//                log.debug("Cache miss for key: {}. Fetching from MongoDB...", key);
//            }
//        } catch (Exception e) {
//            log.error("Error accessing Memcached for key: {}. Proceeding with MongoDB fetch.", key, e);
//        }

        // Construct MongoDB query
        Query query = new Query();
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId.toUpperCase())
                .and("attributeId").is(attributeId.toUpperCase())
                .and("postingDate").is(postingDate)
                .and("metricName").is(metric.toUpperCase()));

        // Fetch from MongoDB
        AttributeLevelLtd ltd = null;
        try {
            ltd = dataService.findOne(query, tenantId, AttributeLevelLtd.class);
            if (ltd != null) {
                log.info("Successfully retrieved balance from MongoDB for key: {}", key);
            } else {
                log.warn("No balance found in MongoDB for key: {}", key);
            }
        } catch (DataAccessException e) {
            log.error("MongoDB query failed for Instrument: {}, Attribute: {}, Metric: {}, PostingDate: {}",
                    instrumentId, attributeId, metric, postingDate, e);
            return null; // Return null to indicate data retrieval failure
        }

        // Cache the fetched result
        if(ltd != null) {
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
     * @return A list of {@link AttributeLevelLtd} records after aggregation.
     */
    public Collection<AttributeLevelLtd> aggregate(TransactionActivity activity, int previousPostingDate) {
        log.info("Starting aggregation for transaction: {}", activity.getTransactionName());

        List<Aggregation> metrics;
        try {
            metrics = this.aggregationService.getMetrics(activity.getTransactionName().toUpperCase());
        } catch (Exception e) {
            log.error("Failed to fetch metrics for transaction: {}", activity.getTransactionName(), e);
            return new ArrayList<>();
        }

        List<AttributeLevelLtd> aggregates = new ArrayList<>();
        for (Aggregation aggregation : metrics) {
            boolean currentPeriodLtdFound = true;
            String key = getKey(this.dataService.getTenantId(), activity.getPeriodId(), activity.getInstrumentId(), activity.getAttributeId(), aggregation.getMetricName());

            AttributeLevelLtd ltd;
            try {
                ltd = this.getBalance(activity.getInstrumentId(), activity.getAttributeId(), aggregation.getMetricName(), activity.getPostingDate());

                if (ltd == null) {
                    log.warn("No balance found for current period. Fetching from previous period.");
                    ltd = this.getBalance(activity.getInstrumentId(), activity.getAttributeId(), aggregation.getMetricName(), previousPostingDate);
                    currentPeriodLtdFound = false;
                }
            } catch (Exception e) {
                log.error("Error retrieving balance for Instrument: {}, Attribute: {}, Metric: {}, Period: {}",
                        activity.getInstrumentId(), activity.getAttributeId(), aggregation.getMetricName(), activity.getPeriodId(), e);
                continue; // Use continue to skip to the next iteration
            }

            BigDecimal activityAmount = ltd.getBalance().getActivity().add(activity.getAmount());
            BigDecimal endingBalance = activityAmount.add(ltd.getBalance().getBeginningBalance());

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

                AttributeLevelLtd attributeLevelLtd = AttributeLevelLtd.builder()
                        .attributeId(activity.getAttributeId())
                        .accountingPeriodId(activity.getPeriodId())
                        .instrumentId(activity.getInstrumentId())
                        .metricName(aggregation.getMetricName())
                        .postingDate(activity.getPostingDate())
                        .balance(balance)
                        .build();

                try {
                    log.debug("Created new balance for key: {} with beginning balance: {} and ending balance: {}", key, beginningBalance, endingBalance);
                    aggregates.add(attributeLevelLtd);
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
     * @return A list of {@link AttributeLevelLtd} records after aggregation.
     */
    public List<AttributeLevelLtd> aggregate(Collection<TransactionActivity> activities, int previousPostingDate) {
        log.info("Starting batch aggregation for {} transactions", activities.size());

        List<AttributeLevelLtd> aggregates = new ArrayList<>();
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

    public List<AttributeLevelLtd> getBalance(String instrumentId, String attributeId, List<String> metrics, int postingDate) {
        Query query = new Query();
        Set<String> metricList = new HashSet<>(StringUtil.convertUpperCase(metrics));
        // Add criteria to filter by transactionName (list) and transactionDate
        query.addCriteria(Criteria.where("instrumentId").is(instrumentId.toUpperCase())
                .and("attributeId").is(attributeId.toUpperCase())
                .and("postingDate").is(postingDate)
                .and("metricName").in(metricList));

        // Execute the query
        return this.dataService.fetchData(query, AttributeLevelLtd.class);
    }

    /**
     * Generates a unique cache key based on tenant ID, accounting period, instrument, attribute, and metric.
     *
     * @param tenantId           The tenant ID.
     * @param accountingPeriodId The accounting period ID.
     * @param instrumentId       The instrument ID.
     * @param attributeId        The attribute ID.
     * @param metricName         The metric name.
     * @return A unique cache key for the given parameters.
     */

    private String getKey(String tenantId, int accountingPeriodId, String instrumentId, String attributeId, String metricName) {
        String key = String.format("%s-%d-%s-%s-%s",
                tenantId,
                accountingPeriodId,
                instrumentId.toUpperCase(),
                attributeId.toUpperCase(),
                metricName.toUpperCase());
        String cleanedKey = StringUtil.removeSpaces(key);
        return StringUtil.generateSHA256Hash(cleanedKey);
    }
}

