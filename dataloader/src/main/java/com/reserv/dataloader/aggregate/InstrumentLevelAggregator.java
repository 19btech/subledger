package com.reserv.dataloader.aggregate;

import com.reserv.dataloader.entity.InstrumentLevelLtd;
import com.reserv.dataloader.entity.TransactionActivity;
import com.reserv.dataloader.entity.BaseLtd;
import com.reserv.dataloader.entity.Settings;
import com.reserv.dataloader.entity.AccountingPeriod;
import com.reserv.dataloader.key.InstrumentLevelLtdKey;
import com.reserv.dataloader.repository.MemcachedRepository;
import com.reserv.dataloader.service.DataService;
import com.reserv.dataloader.service.SettingsService;
import com.reserv.dataloader.utils.DateUtil;
import com.reserv.dataloader.utils.Key;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
@Slf4j
public class InstrumentLevelAggregator extends BaseAggregator {

    private Set<String> allInstrumentLevelInstruments;
    private Set<Integer> newPeriods;
    private int lastTransactionActitvityAccountingPeriod;

    /**
     * Constructor
     * @param memcachedRepository
     * @param dataService
     * @param settingsService
     * @param tenantId
     */
    public InstrumentLevelAggregator(MemcachedRepository memcachedRepository
            , DataService<InstrumentLevelLtd> dataService
            , SettingsService settingsService
            , String tenantId) {
        super(memcachedRepository
                ,dataService
                ,settingsService
                , tenantId);
        newPeriods = new HashSet<>(0);

        try {
            allInstrumentLevelInstruments = this.memcachedRepository.getFromCache(Key.allInstrumentLevelLtdKeyList(tenantId), Set.class);
            if(allInstrumentLevelInstruments == null) {
                allInstrumentLevelInstruments = new HashSet<>(0);
            }
        } catch (Exception e) {
            log.error("Failed to load allInstrumentLevelInstruments from cache", e);
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

        // Set last accounting period of transaction activity upload
        try {
            Settings settings = this.settingsService.fetch(this.tenantId);
            settings.setLastTransactionActivityUploadReportingPeriod(lastTransactionActitvityAccountingPeriod);
            this.dataService.saveObject(settings, this.tenantId);
        } catch (Exception e) {
            log.error("Failed to update settings with the last transaction activity upload reporting period", e);
        }
    }

    /**
     * Generates carryover aggregated LTD entries for missing instrument for accounting period
     */
    private void generateCarryOverAggregateEntries() {
        log.info("Generating carry over aggregate entries");
        //For remaining instruments that have no activity so we will create
        //a carry over entry for those instruments

        Iterator<String> instrumentIteratror = allInstrumentLevelInstruments.iterator();
        Set<InstrumentLevelLtd> carryOvers = new HashSet<>(0);
        while(instrumentIteratror.hasNext()) {
            String instrumentLevelInstrumentLtdKey = instrumentIteratror.next();
            try {
                if (this.memcachedRepository.ifExists(instrumentLevelInstrumentLtdKey)) {
                    InstrumentLevelLtd instrumentLevelLtd = this.memcachedRepository.getFromCache(instrumentLevelInstrumentLtdKey, InstrumentLevelLtd.class);
                    for (Integer activityAccountingPeriodId : newPeriods) {
                        if (instrumentLevelLtd != null && activityAccountingPeriodId > instrumentLevelLtd.getAccountingPeriodId()) {
                            double beginningBalance = instrumentLevelLtd.getBalance().getBeginningBalance() + instrumentLevelLtd.getBalance().getActivity();
                            BaseLtd balance = BaseLtd.builder()
                                    .beginningBalance(beginningBalance)
                                    .activity(0L).build();
                            InstrumentLevelLtd currentLtd = InstrumentLevelLtd.builder()
                                    .accountingPeriodId(activityAccountingPeriodId)
                                    .instrumentId(instrumentLevelLtd.getInstrumentId())
                                    .metricName(instrumentLevelLtd.getMetricName())
                                    .balance(balance).build();
                            InstrumentLevelLtdKey currentPeriodLtdKey = new InstrumentLevelLtdKey(this.tenantId,
                                    instrumentLevelLtd.getMetricName().toUpperCase(),
                                    instrumentLevelLtd.getInstrumentId(), activityAccountingPeriodId);
                            this.memcachedRepository.putInCache(currentPeriodLtdKey.getKey(), currentLtd);
                            carryOvers.add(currentLtd);
                            this.memcachedRepository.delete(instrumentLevelInstrumentLtdKey);
                            this.ltdObjectCleanupList.add(instrumentLevelInstrumentLtdKey);
                        }
                    }
                    instrumentIteratror.remove();
                }
            } catch (Exception e) {
                log.error("Failed to generate carry over aggregate entries for key: {}", instrumentLevelInstrumentLtdKey, e);
            }
        }

        try {
            this.dataService.saveAll(carryOvers, this.tenantId, InstrumentLevelLtd.class);
        } catch (Exception e) {
            log.error("Failed to save carry over aggregate entries", e);
        }

    }

    /**
     * Aggregate LTD for transaction activity
     * @param activity
     */
    public void aggregate(TransactionActivity activity) {
        Set<InstrumentLevelLtd> balances = new HashSet<>(0);

        Map<String, AccountingPeriod> accountingPeriodMap;
        try {
            accountingPeriodMap = this.memcachedRepository.getFromCache(Key.accountingPeriodKey(this.tenantId), Map.class);
        } catch (Exception e) {
            log.error("Failed to load accounting period map from cache", e);
            return;
        }

        List<String> metrics = this.getMetrics(activity);

        if(metrics == null || metrics.isEmpty()) {
            return;
        }

        String accountingPeriod = DateUtil.getAccountingPeriod(activity.getTransactionDate());
        AccountingPeriod activityAccountingPeriod = accountingPeriodMap.get(accountingPeriod);
        newPeriods.add(activityAccountingPeriod.getPeriodId());
        int activityAccountingPeriodId=0;
        int previousAccountingPeriodId=0;
        if(activityAccountingPeriod == null) {
            return;
        }else if(activityAccountingPeriod.getStatus() != 0) {
            previousAccountingPeriodId = referenceData.getPrevioudAccountingPeriodId();
        } else {
            activityAccountingPeriodId =  activityAccountingPeriod.getPeriodId();
            if(activityAccountingPeriod.getPreviousAccountingPeriod() != null && !activityAccountingPeriod.getPreviousAccountingPeriod().isEmpty()) {
                AccountingPeriod previousAccountingPeriod = accountingPeriodMap.get(activityAccountingPeriod.getPreviousAccountingPeriod());
                previousAccountingPeriodId = previousAccountingPeriod.getPeriodId();
            }
        }

        if(activityAccountingPeriodId > lastTransactionActitvityAccountingPeriod) {
            lastTransactionActitvityAccountingPeriod = activityAccountingPeriodId;
        }

        for(String metric : metrics) {

            try {
                InstrumentLevelLtdKey previousPeriodKey = new InstrumentLevelLtdKey(this.tenantId, metric.toUpperCase(), activity.getInstrumentId(), previousAccountingPeriodId);
                InstrumentLevelLtdKey currentPeriodKey = new InstrumentLevelLtdKey(this.tenantId, metric.toUpperCase(), activity.getInstrumentId(), activityAccountingPeriodId);

                InstrumentLevelLtd currentLtd = this.memcachedRepository.getFromCache(currentPeriodKey.getKey(), InstrumentLevelLtd.class);
                InstrumentLevelLtd previousLtd = this.memcachedRepository.getFromCache(previousPeriodKey.getKey(), InstrumentLevelLtd.class);
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
                        currentLtd.getBalance().setActivity(beginningBalance + activity.getValue());
                        currentLtd.getBalance().setEndingBalance(beginningBalance + activity.getValue());
                    }else {
                        currentLtd.getBalance().setBeginningBalance(beginningBalance);
                        currentLtd.getBalance().setActivity(currentLtd.getBalance().getActivity() + activity.getValue());
                        currentLtd.getBalance().setEndingBalance(beginningBalance + currentLtd.getBalance().getActivity());
                    }

                    balances.add(currentLtd);
                } else {
                    BaseLtd balance = BaseLtd.builder()
                            .beginningBalance(beginningBalance)
                            .activity(activity.getValue()).build();
                    currentLtd = InstrumentLevelLtd.builder()
                            .accountingPeriodId(activityAccountingPeriodId)
                            .instrumentId(activity.getInstrumentId())
                            .metricName(metric)
                            .balance(balance).build();
                    this.memcachedRepository.putInCache(currentPeriodKey.getKey(), currentLtd);
                   // this.memcachedRepository.delete(previousPeriodKey.getKey());
                    this.ltdObjectCleanupList.add(previousPeriodKey.getKey());
                    allInstrumentLevelInstruments.add(currentPeriodKey.getKey());
                    allInstrumentLevelInstruments.remove(previousPeriodKey.getKey());
                    balances.add(currentLtd);
                }
            } catch (Exception e) {
                log.error("Failed to process metric: {}", metric, e);
            }
        }

        Set<InstrumentLevelLtd> updateable = new HashSet<>(0);
        Set<InstrumentLevelLtd> insertable = new HashSet<>(0);
        for(InstrumentLevelLtd instrumentLevelLtd : balances) {

            if(instrumentLevelLtd.getId() != null) {
                updateable.add(instrumentLevelLtd);
            }else{
                insertable.add(instrumentLevelLtd);
            }
        }

        try {
            for (InstrumentLevelLtd ltd : this.dataService.saveAll(insertable, this.tenantId, InstrumentLevelLtd.class)) {
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
            for (InstrumentLevelLtd ltd : updateable) {
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
