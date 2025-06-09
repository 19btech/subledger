package com.reserv.dataloader.aggregate;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.key.AttributeLevelLtdKey;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.SettingsService;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.fyntrac.common.service.aggregation.AttributeLevelAggregationService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.query.Criteria;

import java.math.BigDecimal;
import java.util.*;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

/**
 * Attribute Level Aggregator
 */
@Slf4j
public class AttributeLevelAggregator extends BaseAggregator {

    private Set<String> allAttributeLevelInstruments;
    private Set<Integer> newPostingDates;
    private final ExecutionState executionState;
    private Integer lastActivityPostingDate;
    private Integer activityPostingDate;
    private final AttributeLevelAggregationService attributeLevelAggregationService;

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
                                    , AccountingPeriodService accountingPeriodService
                                    , AggregationService aggregationService
                                    , AttributeLevelAggregationService attributeLevelAggregationService
            , String tenantId) {
        super(memcachedRepository
                ,dataService
                ,settingsService
                , executionStateService
                , accountingPeriodService
                , aggregationService
                , tenantId);
        newPostingDates = new HashSet<>(0);
        this.attributeLevelAggregationService = attributeLevelAggregationService;

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

            }else if(executionState.getLastExecutionDate() !=null && executionState.getLastExecutionDate() > executionState.getLastActivityPostingDate()) {
                lastActivityPostingDate = executionState.getLastExecutionDate();
            }

            List<Records.GroupedMetricsByInstrumentAttribute> metrics = getGroupedDistinctMetricNames(lastActivityPostingDate);
            allAttributeLevelInstruments = this.getDistinctMetricNamesByPostingDate(metrics);
            if(allAttributeLevelInstruments == null) {
                allAttributeLevelInstruments = new HashSet<>(0);
            }
        } catch (Exception e) {
            log.error("Failed to load allAttributeLevelInstruments from cache", e);
            throw new RuntimeException("Initialization failed", e);
        }

    }

    /**
     * getDistinctMetricNamesByPostingDate from previous postings
     * @param metrics
     * @return
     */
    public Set<String> getDistinctMetricNamesByPostingDate(List<Records.GroupedMetricsByInstrumentAttribute> metrics) {
        Set<String> metricSet = new HashSet<>(0);
        for(Records.GroupedMetricsByInstrumentAttribute metric : metrics) {
            AttributeLevelLtdKey previousPeriodKey = new AttributeLevelLtdKey(this.tenantId, metric.metricName(), metric.instrumentId(), metric.attributeId(), lastActivityPostingDate);
            metricSet.add(previousPeriodKey.getKey());

        }
        return metricSet;
    }

    public List<Records.GroupedMetricsByInstrumentAttribute> getGroupedDistinctMetricNames(int postingDate) {
        MatchOperation matchStage = match(Criteria.where("postingDate").is(postingDate));

        GroupOperation groupStage = group("instrumentId", "attributeId", "metricName")
                .first("instrumentId").as("instrumentId")
                .first("attributeId").as("attributeId")
                .first("metricName").as("metricName");
        ProjectionOperation projectStage = project("instrumentId", "attributeId", "metricName");

        Aggregation aggregation = newAggregation(matchStage, groupStage, projectStage);

        return this.dataService.getMongoTemplate().aggregate(aggregation, "AttributeLevelLtd", Records.GroupedMetricsByInstrumentAttribute.class).getMappedResults();
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

        Iterator<String> instrumentIteratror = allAttributeLevelInstruments.iterator();
        Set<AttributeLevelLtd> carryOvers = new HashSet<>(0);
        Map<Integer, Integer> accountingPeriodMap = new HashMap<>(0);

        for(Integer postingDate : newPostingDates) {
            Date pDate = DateUtil.convertToDateFromYYYYMMDD(postingDate);
            int aPeriod = DateUtil.getAccountingPeriodId(pDate);
            AccountingPeriod accountingPeriod = this.accountingPeriodService.getAccountingPeriod(aPeriod, this.tenantId);
            accountingPeriodMap.put(postingDate, accountingPeriod.getPeriodId());
        }
        while(instrumentIteratror.hasNext()) {
            String attributeLevelInstrumentLtdKey = instrumentIteratror.next();
            try {
                if (this.memcachedRepository.ifExists(attributeLevelInstrumentLtdKey)) {
                    AttributeLevelLtd attributeLevelLtd = this.memcachedRepository.getFromCache(attributeLevelInstrumentLtdKey, AttributeLevelLtd.class);
                    for (Integer postingDate : newPostingDates) {
                        if (attributeLevelLtd != null && postingDate > attributeLevelLtd.getPostingDate()) {
                            BigDecimal beginningBalance = attributeLevelLtd.getBalance().getBeginningBalance().add(attributeLevelLtd.getBalance().getActivity());
                            BaseLtd balance = BaseLtd.builder()
                                    .beginningBalance(beginningBalance)
                                    .activity(BigDecimal.valueOf(0L)).build();
                            AttributeLevelLtd currentLtd = AttributeLevelLtd.builder()
                                    .postingDate(postingDate)
                                    .accountingPeriodId(accountingPeriodMap.get(postingDate))
                                    .instrumentId(attributeLevelLtd.getInstrumentId())
                                    .attributeId(attributeLevelLtd.getAttributeId())
                                    .metricName(attributeLevelLtd.getMetricName())
                                    .balance(balance).build();
                            AttributeLevelLtdKey currentPeriodLtdKey = new AttributeLevelLtdKey(this.tenantId,
                                    attributeLevelLtd.getMetricName().toUpperCase(),
                                    attributeLevelLtd.getInstrumentId(),
                                    attributeLevelLtd.getAttributeId(), postingDate);
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

                AttributeLevelLtdKey previousPeriodKey = new AttributeLevelLtdKey(this.tenantId, metric.toUpperCase(), activity.getInstrumentId(), activity.getAttributeId(), executionState.getLastActivityPostingDate());
                AttributeLevelLtdKey currentPeriodKey = new AttributeLevelLtdKey(this.tenantId, metric.toUpperCase(), activity.getInstrumentId(), activity.getAttributeId(), executionState.getActivityPostingDate());

                AttributeLevelLtd currentLtd = this.attributeLevelAggregationService.getBalance(this.tenantId, activity.getInstrumentId(), activity.getAttributeId(), metric.toUpperCase(), activityPostingDate);
                AttributeLevelLtd previousLtd = this.attributeLevelAggregationService.getBalance(this.tenantId, activity.getInstrumentId(), activity.getAttributeId(), metric.toUpperCase(), lastActivityPostingDate);
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
            }{
                this.memcachedRepository.putInCache(Key.allAttributeLevelLtdKeyList(tenantId), allAttributeLevelInstruments);
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

