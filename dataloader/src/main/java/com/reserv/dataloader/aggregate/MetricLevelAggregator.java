package com.reserv.dataloader.aggregate;

import com.fyntrac.common.entity.BaseLtd;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.key.MetricLevelLtdKey;
import com.fyntrac.common.entity.MetricLevelLtd;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import com.fyntrac.common.service.SettingsService;

import java.util.*;
@Slf4j
public class MetricLevelAggregator  extends BaseAggregator {

    private Set<String> allMetricLevelInstruments;
    private Set<Integer> newPeriods;
    private int lastTransactionActitvityAccountingPeriod;

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
            , String tenantId) {
        super(memcachedRepository
                ,dataService
                ,settingsService
                , tenantId);
        newPeriods = new HashSet<>(0);

        try {
            allMetricLevelInstruments = this.memcachedRepository.getFromCache(Key.allMetricLevelLtdKeyList(tenantId), Set.class);
            if(allMetricLevelInstruments == null) {
                allMetricLevelInstruments = new HashSet<>(0);
            }
        } catch (Exception e) {
            log.error("Failed to load allMetricLevelInstruments from cache", e);
            throw new RuntimeException("Initialization failed", e);
        }
        try {
            this.lastTransactionActitvityAccountingPeriod = this.settingsService.fetch().getLastTransactionActivityUploadReportingPeriod();
        } catch (Exception e) {
            log.error("Failed to fetch lastTransactionActivityUploadReportingPeriod from settingsService", e);
            throw new RuntimeException("Initialization failed", e);
        }

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
            this.generateCarryOverAggregateEntries();
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
        while(instrumentIteratror.hasNext()) {
            String metricLevelLtdKey = instrumentIteratror.next();
            try {
                if (this.memcachedRepository.ifExists(metricLevelLtdKey)) {
                    MetricLevelLtd metricLevelLtd = this.memcachedRepository.getFromCache(metricLevelLtdKey, MetricLevelLtd.class);
                    for (Integer activityAccountingPeriodId : newPeriods) {
                        if (metricLevelLtd != null && activityAccountingPeriodId > metricLevelLtd.getAccountingPeriodId()) {
                            double beginningBalance = metricLevelLtd.getBalance().getBeginningBalance() + metricLevelLtd.getBalance().getActivity();
                            BaseLtd balance = BaseLtd.builder()
                                    .beginningBalance(beginningBalance)
                                    .activity(0L).build();
                            MetricLevelLtd currentLtd = MetricLevelLtd.builder()
                                    .accountingPeriodId(activityAccountingPeriodId)
                                    .metricName(metricLevelLtd.getMetricName())
                                    .balance(balance).build();
                            MetricLevelLtdKey currentPeriodLtdKey = new MetricLevelLtdKey(this.tenantId,
                                    metricLevelLtd.getMetricName().toUpperCase()
                                    , activityAccountingPeriodId);
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

        List<String> metrics = this.getMetrics(activity);

        if(metrics == null || metrics.isEmpty()) {
            return;
        }

        AccountingPeriod activityAccountingPeriod = activity.getAccountingPeriod();
        newPeriods.add(activityAccountingPeriod.getPeriodId());
        int activityAccountingPeriodId=0;
        int previousAccountingPeriodId=0;
        if(activityAccountingPeriod == null) {
            return;
        }else if(activityAccountingPeriod.getStatus() != 0) {
            previousAccountingPeriodId = referenceData.getPrevioudAccountingPeriodId();
        } else {
            activityAccountingPeriodId =  activityAccountingPeriod.getPeriodId();
            if(activityAccountingPeriod.getPreviousAccountingPeriodId() != 0) {
                previousAccountingPeriodId = activityAccountingPeriod.getPreviousAccountingPeriodId();
            }
        }

        if(activityAccountingPeriodId > lastTransactionActitvityAccountingPeriod) {
            lastTransactionActitvityAccountingPeriod = activityAccountingPeriodId;
        }

        for(String metric : metrics) {

            try {
                MetricLevelLtdKey previousPeriodKey = new MetricLevelLtdKey(this.tenantId, metric.toUpperCase(), previousAccountingPeriodId);
                MetricLevelLtdKey currentPeriodKey = new MetricLevelLtdKey(this.tenantId, metric.toUpperCase(), activityAccountingPeriodId);

                MetricLevelLtd currentLtd = this.memcachedRepository.getFromCache(currentPeriodKey.getKey(), MetricLevelLtd.class);
                MetricLevelLtd previousLtd = this.memcachedRepository.getFromCache(previousPeriodKey.getKey(), MetricLevelLtd.class);
                double beginningBalance = 0.0d;
                boolean ifPreviousLtdExists = Boolean.FALSE;
                if (previousLtd != null) {
                    beginningBalance = previousLtd.getBalance().getBeginningBalance() + previousLtd.getBalance().getActivity();
                    ifPreviousLtdExists = Boolean.TRUE;
                }

                if (currentLtd != null) {
                    if(!ifPreviousLtdExists) {
                        beginningBalance = +currentLtd.getBalance().getBeginningBalance() + currentLtd.getBalance().getActivity();
                        currentLtd.getBalance().setBeginningBalance(0);
                        currentLtd.getBalance().setActivity(beginningBalance + activity.getAmount());
                        currentLtd.getBalance().setEndingBalance(beginningBalance + activity.getAmount());
                    }else {
                        currentLtd.getBalance().setBeginningBalance(beginningBalance);
                        currentLtd.getBalance().setActivity(currentLtd.getBalance().getActivity() + activity.getAmount());
                        currentLtd.getBalance().setEndingBalance(beginningBalance + currentLtd.getBalance().getActivity());
                    }

                    balances.add(currentLtd);
                } else {
                    BaseLtd balance = BaseLtd.builder()
                            .beginningBalance(beginningBalance)
                            .activity(activity.getAmount()).build();
                    currentLtd = MetricLevelLtd.builder()
                            .accountingPeriodId(activityAccountingPeriodId)
                            .metricName(metric)
                            .balance(balance).build();
                    this.memcachedRepository.putInCache(currentPeriodKey.getKey(), currentLtd);
                    // this.memcachedRepository.delete(previousPeriodKey.getKey());
                    this.ltdObjectCleanupList.add(previousPeriodKey.getKey());
                    allMetricLevelInstruments.add(currentPeriodKey.getKey());
                    allMetricLevelInstruments.remove(previousPeriodKey.getKey());
                    balances.add(currentLtd);
                }
            } catch (Exception e) {
                log.error("Failed to process metric: {}", metric, e);
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
