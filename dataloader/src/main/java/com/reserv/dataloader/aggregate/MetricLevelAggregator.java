package com.reserv.dataloader.aggregate;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.key.MetricLevelLtdKey;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.fyntrac.common.service.aggregation.MetricLevelAggregationService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.Key;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import com.fyntrac.common.service.SettingsService;
import org.springframework.data.mongodb.core.query.Criteria;

import org.springframework.data.mongodb.core.query.Query;

import java.math.BigDecimal;
import java.util.*;
@Slf4j
public class MetricLevelAggregator  extends BaseAggregator {

    private Set<String> allMetricLevelInstruments;
    private Set<Integer> newPostingDates;
    private Integer lastActivityPostingDate;
    private Integer activityPostingDate;
    private final MetricLevelAggregationService metricLevelAggregationService;

    /**
     * Constructor
     * @param memcachedRepository
     * @param dataService
     * @param settingsService
     * @param tenantId
     */
    public MetricLevelAggregator(MemcachedRepository memcachedRepository
            , DataService<MetricLevelLtd> dataService
            , SettingsService settingsService
                                 , AccountingPeriodService accountingPeriodService
                                 , AggregationService aggregationService
                                 , MetricLevelAggregationService metricLevelAggregationService
                                 , AggregationRequest aggregationRequest
            , String tenantId) {
        super(memcachedRepository
                ,dataService
                ,settingsService
                , accountingPeriodService
                , aggregationService
                , aggregationRequest
                , tenantId);
        newPostingDates = new HashSet<>(0);
        this.metricLevelAggregationService =metricLevelAggregationService;

        try {


            lastActivityPostingDate = this.aggregationRequest.getLastPostingDate();
            activityPostingDate = this.aggregationRequest.getPostingDate();

            allMetricLevelInstruments = this.getDistinctMetricNamesByPostingDate(lastActivityPostingDate);
            if(allMetricLevelInstruments == null) {
                allMetricLevelInstruments = new HashSet<>(0);
            }



        } catch (Exception e) {
            log.error("Failed to load allMetricLevelInstruments from cache", e);
            throw new RuntimeException("Initialization failed", e);
        }


    }

    /**
     * getDistinctMetricNamesByPostingDate from previous postings
     * @param postingDate
     * @return
     */
    public Set<String> getDistinctMetricNamesByPostingDate(int postingDate) {
        Query query = new Query(Criteria.where("postingDate").is(postingDate));
        List<String> metrics = this.dataService.getMongoTemplate().findDistinct(query, "metricName", "MetricLevelLtd", String.class);
        Set<String> metricSet = new HashSet<>(0);
        for(String metric : metrics) {
            MetricLevelLtdKey previousPeriodKey = new MetricLevelLtdKey(this.tenantId, metric.toUpperCase(), postingDate);
            metricSet.add(previousPeriodKey.getKey());

        }
        return metricSet;
    }

    /**
     * Aggregate Transaction Activities
     * @param activities
     */
    public void aggregate(List<String> activities) {
        for(String transactionActivityKey : activities) {
            try {
                TransactionActivity transactionActivity = this.memcachedRepository.getFromCache(transactionActivityKey, TransactionActivity.class);
                this.aggregate(transactionActivity);
            } catch (Exception e) {
                log.error("Failed to process transactionActivityKey: {}", transactionActivityKey, e);
            }
        }

        try {
            // this.generateCarryOverAggregateEntries();
        } catch (Exception e) {
            log.error("Failed to generate carry over aggregate entries", e);
        }
    }

    /**
     * Generates carryover aggregated LTD entries for missing instrument for accounting period
     */
    private void generateCarryOverAggregateEntries() {
        log.info("Generating carry over aggregate entries");
        //For remaining instruments that have no activity so we will create
        //a carry over entry for those instruments

        Iterator<String> instrumentIteratror = allMetricLevelInstruments.iterator();
        Set<MetricLevelLtd> carryOvers = new HashSet<>(0);
        Map<Integer, Integer> accountingPeriodMap = new HashMap<>(0);

        for(Integer postingDate : newPostingDates) {
            Date pDate = DateUtil.convertToDateFromYYYYMMDD(postingDate);
            int aPeriod = DateUtil.getAccountingPeriodId(pDate);
            AccountingPeriod accountingPeriod = this.accountingPeriodService.getAccountingPeriod(aPeriod, this.tenantId);
            accountingPeriodMap.put(postingDate, accountingPeriod.getPeriodId());
        }

        while(instrumentIteratror.hasNext()) {
            String metricLevelLtdKey = instrumentIteratror.next();
            try {
                if (this.memcachedRepository.ifExists(metricLevelLtdKey)) {
                    MetricLevelLtd metricLevelLtd = this.memcachedRepository.getFromCache(metricLevelLtdKey, MetricLevelLtd.class);
                    for (Integer postingDate : newPostingDates) {
                        if (metricLevelLtd != null && postingDate > metricLevelLtd.getPostingDate()) {
                            BigDecimal beginningBalance = metricLevelLtd.getBalance().getBeginningBalance().add(metricLevelLtd.getBalance().getActivity());
                            BaseLtd balance = BaseLtd.builder()
                                    .beginningBalance(beginningBalance)
                                    .activity(BigDecimal.valueOf(0L)).build();
                            MetricLevelLtd currentLtd = MetricLevelLtd.builder()
                                    .postingDate(postingDate)
                                    .accountingPeriodId(accountingPeriodMap.get(postingDate))
                                    .metricName(metricLevelLtd.getMetricName())
                                    .balance(balance).build();
                            MetricLevelLtdKey currentPeriodLtdKey = new MetricLevelLtdKey(this.tenantId,
                                    metricLevelLtd.getMetricName().toUpperCase()
                                    , postingDate);
                            this.memcachedRepository.putInCache(currentPeriodLtdKey.getKey(), currentLtd);
                            carryOvers.add(currentLtd);
                            this.memcachedRepository.delete(metricLevelLtdKey);
                            this.ltdObjectCleanupList.add(metricLevelLtdKey);
                        }
                    }
                    instrumentIteratror.remove();
                }
            } catch (Exception e) {
                log.error("Failed to generate carry over aggregate entries for key: {}", metricLevelLtdKey, e);
            }
        }

        try {
            this.dataService.saveAll(carryOvers, this.tenantId, MetricLevelLtd.class);
        } catch (Exception e) {
            log.error("Failed to save carry over aggregate entries", e);
        }

    }

    /**
     * Aggregate LTD for transaction activity
     * @param activity
     */
    public void aggregate(TransactionActivity activity) {
        Set<MetricLevelLtd> balances = new HashSet<>(0);


        if(this.aggregationRequest == null || activity == null) {
            return;
        }
        List<String> metrics = this.getMetrics(activity);

        if(metrics == null || metrics.isEmpty()) {
            TenantContextHolder.setTenant(super.tenantId);
            this.aggregationService.loadIntoCache();
            metrics = this.getMetrics(activity);
        }

        newPostingDates.add(activity.getPostingDate());
        for(String metric : metrics) {

            try {



                MetricLevelLtdKey previousPeriodKey = new MetricLevelLtdKey(this.tenantId, metric.toUpperCase(), lastActivityPostingDate);
                MetricLevelLtdKey currentPeriodKey = new MetricLevelLtdKey(this.tenantId, metric.toUpperCase(), activityPostingDate);

                MetricLevelLtd currentLtd = this.metricLevelAggregationService.getBalance(this.tenantId, metric.toUpperCase(), activityPostingDate);
                MetricLevelLtd previousLtd = this.metricLevelAggregationService.getBalance(this.tenantId, metric.toUpperCase(), lastActivityPostingDate);

                BigDecimal beginningBalance = BigDecimal.valueOf(0L);
                if (previousLtd != null) {
                    beginningBalance = previousLtd.getBalance().getBeginningBalance().add(previousLtd.getBalance().getActivity());
                }

                if (currentLtd != null) {
                        currentLtd.getBalance().setActivity(currentLtd.getBalance().getActivity().add(activity.getAmount()));
                        currentLtd.getBalance().setEndingBalance(currentLtd.getBalance().getBeginningBalance().add(currentLtd.getBalance().getActivity()));
                    balances.add(currentLtd);
                } else {
                    BaseLtd balance = BaseLtd.builder()
                            .beginningBalance(beginningBalance)
                            .activity(activity.getAmount()).build();
                    currentLtd = MetricLevelLtd.builder()
                            .postingDate(activity.getPostingDate())
                            .accountingPeriodId(activity.getPeriodId())
                            .metricName(metric)
                            .balance(balance).build();
                    this.memcachedRepository.putInCache(currentPeriodKey.getKey().trim(), currentLtd);
                    // this.memcachedRepository.delete(previousPeriodKey.getKey());
                    this.ltdObjectCleanupList.add(previousPeriodKey.getKey());
                    allMetricLevelInstruments.add(currentPeriodKey.getKey());
                    allMetricLevelInstruments.remove(previousPeriodKey.getKey());
                    balances.add(currentLtd);
                }
            } catch (Exception e) {
                log.error("Failed to process metric: {}", metric, StringUtil.getStackTrace(e));
            }finally {
                this.memcachedRepository.putInCache(Key.allMetricLevelLtdKeyList(tenantId), allMetricLevelInstruments);
            }
        }

        Set<MetricLevelLtd> updateable = new HashSet<>(0);
        Set<MetricLevelLtd> insertable = new HashSet<>(0);
        for(MetricLevelLtd metricLevelLtd : balances) {

            if(metricLevelLtd.getId() != null) {
                updateable.add(metricLevelLtd);
            }else{
                insertable.add(metricLevelLtd);
            }
        }

        try {
            for (MetricLevelLtd ltd : this.dataService.saveAll(insertable, this.tenantId, MetricLevelLtd.class)) {
                String k = ltd.getKey(this.tenantId);
                if (this.memcachedRepository.ifExists(k)) {
                    this.memcachedRepository.replaceInCache(k, ltd);
                } else {
                    this.memcachedRepository.putInCache(k, ltd);
                    this.ltdObjectCleanupList.add(k);
                }
            }
        }catch(Exception e){
            log.error("Failed to process metric: {}", insertable, e);
        }

        try {
            for (MetricLevelLtd ltd : updateable) {
                this.dataService.saveObject(ltd, this.tenantId);
                String k = ltd.getKey(this.tenantId);
                if (this.memcachedRepository.ifExists(k)) {
                    this.memcachedRepository.replaceInCache(k, ltd);
                } else {
                    this.memcachedRepository.putInCache(k, ltd);
                    this.ltdObjectCleanupList.add(k);
                }
            }
        }catch(Exception e) {
            log.error("Failed to process metric: {}", updateable, e);
        }
    }
}
