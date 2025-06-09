package com.reserv.dataloader.aggregate;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.key.AttributeLevelLtdKey;
import com.fyntrac.common.key.InstrumentLevelLtdKey;
import com.fyntrac.common.key.MetricLevelLtdKey;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.SettingsService;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.fyntrac.common.service.aggregation.InstrumentLevelAggregationService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.math.BigDecimal;
import java.util.*;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;

@Slf4j
public class InstrumentLevelAggregator extends BaseAggregator {

    private Set<String> allInstrumentLevelInstruments;
    private Set<Integer> newPostingDates;
    private final ExecutionState executionState;
    private Integer lastActivityPostingDate;
    private Integer activityPostingDate;
    private final InstrumentLevelAggregationService instrumentLevelAggregationService;
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
                                     , ExecutionStateService executionStateService
                                     , AccountingPeriodService accountingPeriodService
                                     , AggregationService aggregationService
                                     , InstrumentLevelAggregationService instrumentLevelAggregationService
            , String tenantId) {
        super(memcachedRepository
                ,dataService
                ,settingsService
                , executionStateService
                , accountingPeriodService
                , aggregationService
                , tenantId);
        newPostingDates = new HashSet<>(0);
        this.instrumentLevelAggregationService = instrumentLevelAggregationService;

        try {
            this.executionState = this.getExecutionState();

            lastActivityPostingDate = executionState.getLastActivityPostingDate();
            activityPostingDate = executionState.getActivityPostingDate();
            if(executionState.getExecutionDate() != null && executionState.getExecutionDate() > executionState.getActivityPostingDate()) {
                lastActivityPostingDate = executionState.getActivityPostingDate();
                activityPostingDate = executionState.getExecutionDate();

                if(executionState.getLastExecutionDate() != null && executionState.getLastExecutionDate() > lastActivityPostingDate) {
                    lastActivityPostingDate = executionState.getLastExecutionDate();
                }
            }else if (executionState.getExecutionDate() != null && executionState.getExecutionDate() < executionState.getActivityPostingDate()) {
                lastActivityPostingDate = executionState.getExecutionDate();
                activityPostingDate = executionState.getActivityPostingDate();

            }else if(executionState.getLastExecutionDate() !=null &&  executionState.getLastExecutionDate() !=null && executionState.getLastExecutionDate() > executionState.getLastActivityPostingDate()) {
                lastActivityPostingDate = executionState.getLastExecutionDate();
            }

            List<Records.GroupedMetricsByInstrument> metrics = getGroupedDistinctMetricNames(lastActivityPostingDate);
            allInstrumentLevelInstruments = this.getDistinctMetricNamesByPostingDate(metrics);
            if(allInstrumentLevelInstruments == null) {
                allInstrumentLevelInstruments = new HashSet<>(0);
            }

        } catch (Exception e) {
            log.error("Failed to load allInstrumentLevelInstruments from cache", e);
            throw new RuntimeException("Initialization failed", e);
        }
    }

    /**
     * getDistinctMetricNamesByPostingDate from previous postings
     * @param metrics
     * @return
     */
    public Set<String> getDistinctMetricNamesByPostingDate(List<Records.GroupedMetricsByInstrument> metrics) {
        Set<String> metricSet = new HashSet<>(0);
        for(Records.GroupedMetricsByInstrument metric : metrics) {
            InstrumentLevelLtdKey previousPeriodKey = new InstrumentLevelLtdKey(this.tenantId, metric.metricName(), metric.instrumentId(), lastActivityPostingDate);
            metricSet.add(previousPeriodKey.getKey());

        }
        return metricSet;
    }

    public List<Records.GroupedMetricsByInstrument> getGroupedDistinctMetricNames(int postingDate) {
        MatchOperation matchStage = match(Criteria.where("postingDate").is(postingDate));

        GroupOperation groupStage = group("instrumentId",  "metricName")
                .first("instrumentId").as("instrumentId")
                .first("metricName").as("metricName");
        ProjectionOperation projectStage = project("instrumentId",  "metricName");

        Aggregation aggregation = newAggregation(matchStage, groupStage, projectStage);

        return this.dataService.getMongoTemplate().aggregate(aggregation, "InstrumentLevelLtd", Records.GroupedMetricsByInstrument.class).getMappedResults();
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

        Iterator<String> instrumentIteratror = allInstrumentLevelInstruments.iterator();
        Set<InstrumentLevelLtd> carryOvers = new HashSet<>(0);

        Map<Integer, Integer> accountingPeriodMap = new HashMap<>(0);

        for(Integer postingDate : newPostingDates) {
            Date pDate = DateUtil.convertToDateFromYYYYMMDD(postingDate);
            int aPeriod = DateUtil.getAccountingPeriodId(pDate);
            AccountingPeriod accountingPeriod = this.accountingPeriodService.getAccountingPeriod(aPeriod, this.tenantId);
            accountingPeriodMap.put(postingDate, accountingPeriod.getPeriodId());
        }

        while(instrumentIteratror.hasNext()) {
            String instrumentLevelInstrumentLtdKey = instrumentIteratror.next();
            try {
                if (this.memcachedRepository.ifExists(instrumentLevelInstrumentLtdKey)) {
                    InstrumentLevelLtd instrumentLevelLtd = this.memcachedRepository.getFromCache(instrumentLevelInstrumentLtdKey, InstrumentLevelLtd.class);
                    for (Integer postingDate : newPostingDates) {
                        if (instrumentLevelLtd != null && postingDate > instrumentLevelLtd.getPostingDate()) {
                            BigDecimal beginningBalance = instrumentLevelLtd.getBalance().getBeginningBalance().add(instrumentLevelLtd.getBalance().getActivity());
                            BaseLtd balance = BaseLtd.builder()
                                    .beginningBalance(beginningBalance)
                                    .activity(BigDecimal.valueOf(0L)).build();
                            InstrumentLevelLtd currentLtd = InstrumentLevelLtd.builder()
                                    .postingDate(postingDate)
                                    .accountingPeriodId(accountingPeriodMap.get(postingDate))
                                    .instrumentId(instrumentLevelLtd.getInstrumentId())
                                    .metricName(instrumentLevelLtd.getMetricName())
                                    .balance(balance).build();
                            InstrumentLevelLtdKey currentPeriodLtdKey = new InstrumentLevelLtdKey(this.tenantId,
                                    instrumentLevelLtd.getMetricName().toUpperCase(),
                                    instrumentLevelLtd.getInstrumentId(), postingDate);
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

        if(executionState == null || activity == null) {
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


                InstrumentLevelLtdKey previousPeriodKey = new InstrumentLevelLtdKey(this.tenantId, metric.toUpperCase(), activity.getInstrumentId(), lastActivityPostingDate);
                InstrumentLevelLtdKey currentPeriodKey = new InstrumentLevelLtdKey(this.tenantId, metric.toUpperCase(), activity.getInstrumentId(), activityPostingDate);

                InstrumentLevelLtd currentLtd = this.instrumentLevelAggregationService.getBalance(this.tenantId, activity.getInstrumentId(), metric.toUpperCase(), activityPostingDate);
                InstrumentLevelLtd previousLtd = this.instrumentLevelAggregationService.getBalance(this.tenantId, activity.getInstrumentId(), metric.toUpperCase(), lastActivityPostingDate);
                BigDecimal beginningBalance = BigDecimal.valueOf(0L);
                if (previousLtd != null) {
                    beginningBalance = previousLtd.getBalance().getEndingBalance();
                }

                if (currentLtd != null) {
                        currentLtd.getBalance().setActivity(currentLtd.getBalance().getActivity().add(activity.getAmount()));
                        currentLtd.getBalance().setEndingBalance(currentLtd.getBalance().getBeginningBalance().add(currentLtd.getBalance().getActivity()));
                    balances.add(currentLtd);
                } else {
                    BaseLtd balance = BaseLtd.builder()
                            .beginningBalance(beginningBalance)
                            .activity(activity.getAmount()).build();
                    currentLtd = InstrumentLevelLtd.builder()
                            .postingDate(activity.getPostingDate())
                            .accountingPeriodId(activity.getPeriodId())
                            .instrumentId(activity.getInstrumentId())
                            .metricName(metric)
                            .balance(balance).build();
                    this.memcachedRepository.putInCache(currentPeriodKey.getKey().trim(), currentLtd);
                   // this.memcachedRepository.delete(previousPeriodKey.getKey());
                    this.ltdObjectCleanupList.add(previousPeriodKey.getKey());
                    allInstrumentLevelInstruments.add(currentPeriodKey.getKey());
                    allInstrumentLevelInstruments.remove(previousPeriodKey.getKey());
                    balances.add(currentLtd);
                }
            } catch (Exception e) {
                log.error("Failed to process metric: {}", metric, e);
            }finally {
                this.memcachedRepository.putInCache(Key.allInstrumentLevelLtdKeyList(tenantId), allInstrumentLevelInstruments);
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

