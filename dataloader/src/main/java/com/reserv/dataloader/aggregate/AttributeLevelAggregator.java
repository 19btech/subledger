package com.reserv.dataloader.aggregate;

import com.fyntrac.common.entity.*;
import com.fyntrac.common.key.AttributeLevelLtdKey;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import com.fyntrac.common.service.SettingsService;

import java.util.*;
/**
 * Attribute Level Aggregator
 */
@Slf4j
public class AttributeLevelAggregator extends BaseAggregator {

    private Set<String> allAttributeLevelInstruments;
    private Set<Integer> newPeriods;

    /**
     * Constructor
     * @param memcachedRepository
     * @param dataService
     * @param settingsService
     * @param tenantId
     */
    public AttributeLevelAggregator(MemcachedRepository memcachedRepository
            , DataService<AttributeLevelLtd> dataService
            , SettingsService settingsService
                                    , ExecutionStateService executionStateService
            , String tenantId) {
        super(memcachedRepository
                ,dataService
                ,settingsService
                , executionStateService
                , tenantId);
        newPeriods = new HashSet<>(0);

        try {
            allAttributeLevelInstruments = this.memcachedRepository.getFromCache(Key.allAttributeLevelLtdKeyList(tenantId), Set.class);
            if(allAttributeLevelInstruments == null) {
                allAttributeLevelInstruments = new HashSet<>(0);
            }
        } catch (Exception e) {
            log.error("Failed to load allAttributeLevelInstruments from cache", e);
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

        Iterator<String> instrumentIteratror = allAttributeLevelInstruments.iterator();
        Set<AttributeLevelLtd> carryOvers = new HashSet<>(0);
        while(instrumentIteratror.hasNext()) {
            String attributeLevelInstrumentLtdKey = instrumentIteratror.next();
            try {
                if (this.memcachedRepository.ifExists(attributeLevelInstrumentLtdKey)) {
                    AttributeLevelLtd attributeLevelLtd = this.memcachedRepository.getFromCache(attributeLevelInstrumentLtdKey, AttributeLevelLtd.class);
                    for (Integer activityAccountingPeriodId : newPeriods) {
                        if (attributeLevelLtd != null && activityAccountingPeriodId > attributeLevelLtd.getAccountingPeriodId()) {
                            double beginningBalance = attributeLevelLtd.getBalance().getBeginningBalance() + attributeLevelLtd.getBalance().getActivity();
                            BaseLtd balance = BaseLtd.builder()
                                    .beginningBalance(beginningBalance)
                                    .activity(0L).build();
                            AttributeLevelLtd currentLtd = AttributeLevelLtd.builder()
                                    .accountingPeriodId(activityAccountingPeriodId)
                                    .instrumentId(attributeLevelLtd.getInstrumentId())
                                    .attributeId(attributeLevelLtd.getAttributeId())
                                    .metricName(attributeLevelLtd.getMetricName())
                                    .balance(balance).build();
                            AttributeLevelLtdKey currentPeriodLtdKey = new AttributeLevelLtdKey(this.tenantId,
                                    attributeLevelLtd.getMetricName().toUpperCase(),
                                    attributeLevelLtd.getInstrumentId(),
                                    attributeLevelLtd.getAttributeId(), activityAccountingPeriodId);
                            this.memcachedRepository.putInCache(currentPeriodLtdKey.getKey(), currentLtd);
                            carryOvers.add(currentLtd);
                            this.memcachedRepository.delete(attributeLevelInstrumentLtdKey);
                            this.ltdObjectCleanupList.add(attributeLevelInstrumentLtdKey);
                        }
                    }
                    instrumentIteratror.remove();
                }
            } catch (Exception e) {
                log.error("Failed to generate carry over aggregate entries for key: {}", attributeLevelInstrumentLtdKey, e);
            }
        }

        try {
            this.dataService.saveAll(carryOvers, this.tenantId, AttributeLevelLtd.class);
        } catch (Exception e) {
            log.error("Failed to save carry over aggregate entries", e);
        }

    }

    /**
     * Aggregate LTD for transaction activity
     * @param activity
     */
    public void aggregate(TransactionActivity activity) {
        Set<AttributeLevelLtd> balances = new HashSet<>(0);

        List<String> metrics = this.getMetrics(activity);
        ExecutionState executionState = this.getExecutionState();
        if(executionState == null || (metrics == null || metrics.isEmpty())) {
            return;
        }


        for(String metric : metrics) {

            try {
                AttributeLevelLtdKey previousPeriodKey = new AttributeLevelLtdKey(this.tenantId, metric.toUpperCase(), activity.getInstrumentId(), activity.getAttributeId(), executionState.getLastActivityPostingDate());
                AttributeLevelLtdKey currentPeriodKey = new AttributeLevelLtdKey(this.tenantId, metric.toUpperCase(), activity.getInstrumentId(), activity.getAttributeId(), executionState.getActivityPostingDate());

                AttributeLevelLtd currentLtd = this.memcachedRepository.getFromCache(currentPeriodKey.getKey(), AttributeLevelLtd.class);
                AttributeLevelLtd previousLtd = this.memcachedRepository.getFromCache(previousPeriodKey.getKey(), AttributeLevelLtd.class);
                double beginningBalance = 0.0d;
                boolean ifPreviousLtdExists = Boolean.FALSE;
                if (previousLtd != null) {
                    beginningBalance = previousLtd.getBalance().getEndingBalance();
                    ifPreviousLtdExists = Boolean.TRUE;
                }

                if (currentLtd != null) {
                    if(!ifPreviousLtdExists) {
                        beginningBalance = +currentLtd.getBalance().getBeginningBalance() + currentLtd.getBalance().getActivity();
                        currentLtd.getBalance().setBeginningBalance(0);
                        currentLtd.getBalance().setActivity(beginningBalance + activity.getAmount());
                        currentLtd.getBalance().setEndingBalance(beginningBalance + activity.getAmount());
                    } else {
                        currentLtd.getBalance().setBeginningBalance(beginningBalance);
                        currentLtd.getBalance().setActivity(currentLtd.getBalance().getActivity() + activity.getAmount());
                        currentLtd.getBalance().setEndingBalance(beginningBalance + currentLtd.getBalance().getActivity());
                    }
                    balances.add(currentLtd);
                } else {
                    BaseLtd balance = BaseLtd.builder()
                            .beginningBalance(beginningBalance)
                            .activity(activity.getAmount()).build();
                    currentLtd = AttributeLevelLtd.builder()
                            .postingDate(activity.getPostingDate())
                            .accountingPeriodId(activity.getPeriodId())
                            .instrumentId(activity.getInstrumentId())
                            .attributeId(activity.getAttributeId())
                            .metricName(metric)
                            .balance(balance).build();
                    this.memcachedRepository.putInCache(currentPeriodKey.getKey().trim(), currentLtd);
                    // this.memcachedRepository.delete(previousPeriodKey.getKey());
                    this.ltdObjectCleanupList.add(previousPeriodKey.getKey());
                    allAttributeLevelInstruments.add(currentPeriodKey.getKey());
                    allAttributeLevelInstruments.remove(previousPeriodKey.getKey());
                    balances.add(currentLtd);
                }
            } catch (Exception e) {
                log.error("Failed to process metric: {}", metric, e);
            }
        }

        Set<AttributeLevelLtd> updateable = new HashSet<>(0);
        Set<AttributeLevelLtd> insertable = new HashSet<>(0);
        for(AttributeLevelLtd attributeLevelLtd : balances) {

            if(attributeLevelLtd.getId() != null) {
                updateable.add(attributeLevelLtd);
            }else{
                insertable.add(attributeLevelLtd);
            }
        }

        try {
            for (AttributeLevelLtd ltd : this.dataService.saveAll(insertable, this.tenantId, AttributeLevelLtd.class)) {
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
            for (AttributeLevelLtd ltd : updateable) {
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

