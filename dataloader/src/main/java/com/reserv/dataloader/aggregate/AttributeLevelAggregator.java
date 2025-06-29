package com.reserv.dataloader.aggregate;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.key.AttributeLevelLtdKey;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.DataService;
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
                                    , AccountingPeriodService accountingPeriodService
                                    , AggregationService aggregationService
                                    , AttributeLevelAggregationService attributeLevelAggregationService
                                    , AggregationRequest aggregationRequest
            , String tenantId
    , long jobId) {
        super(memcachedRepository
                ,dataService
                ,settingsService
                , accountingPeriodService
                , aggregationService
                , aggregationRequest
                , tenantId
        , jobId);
        newPostingDates = new HashSet<>(0);
        this.attributeLevelAggregationService = attributeLevelAggregationService;

        try {

            lastActivityPostingDate = this.aggregationRequest.getLastPostingDate();
            activityPostingDate = this.aggregationRequest.getPostingDate();



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
    public void aggregate(List<TransactionActivity> activities) {

        for(TransactionActivity transactionActivity : activities) {
            try {
                this.aggregate(transactionActivity);
            } catch (Exception e) {
                log.error("Failed to process transactionActivity: {}", transactionActivity.toString(), e);
            }
        }

        try {
           // this.generateCarryOverAggregateEntries();
        } catch (Exception e) {
            log.error("Failed to generate carry over aggregate entries", e);
        }


    }


    /**
     * Aggregate LTD for transaction activity
     * @param activity
     */
    public void aggregate(TransactionActivity activity) {
        Set<AttributeLevelLtd> balances = new HashSet<>(0);

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

                AttributeLevelLtdKey previousPeriodKey = new AttributeLevelLtdKey(String.format("tenantId:%s:jobId:%d",this.tenantId, this.jobId), metric.toUpperCase(), activity.getInstrumentId(), activity.getAttributeId(), this.aggregationRequest.getLastPostingDate());
                AttributeLevelLtdKey currentPeriodKey = new AttributeLevelLtdKey(String.format("tenantId:%s:jobId:%d",this.tenantId, this.jobId), metric.toUpperCase(), activity.getInstrumentId(), activity.getAttributeId(), this.aggregationRequest.getPostingDate());

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
        this.dataService.bulkSave(balances, this.tenantId, AttributeLevelLtd.class);
//        Set<AttributeLevelLtd> updateable = new HashSet<>(0);
//        Set<AttributeLevelLtd> insertable = new HashSet<>(0);
//        for(AttributeLevelLtd attributeLevelLtd : balances) {
//
//            if(attributeLevelLtd.getId() != null) {
//                updateable.add(attributeLevelLtd);
//            }else{
//                insertable.add(attributeLevelLtd);
//            }
//        }
//
//        try {
//            for (AttributeLevelLtd ltd : this.dataService.saveAll(insertable, this.tenantId, AttributeLevelLtd.class)) {
//                String k = ltd.getKey(this.tenantId);
//                if (this.memcachedRepository.ifExists(k)) {
//                    this.memcachedRepository.replaceInCache(k, ltd);
//                } else {
//                    this.memcachedRepository.putInCache(k, ltd);
//                    this.ltdObjectCleanupList.add(k);
//                }
//            }
//        }catch(Exception e){
//            log.error("Failed to process metric: {}", insertable, e);
//        }
//
//        try {
//            for (AttributeLevelLtd ltd : updateable) {
//                this.dataService.saveObject(ltd, this.tenantId);
//                String k = ltd.getKey(this.tenantId);
//                if (this.memcachedRepository.ifExists(k)) {
//                    this.memcachedRepository.replaceInCache(k, ltd);
//                } else {
//                    this.memcachedRepository.putInCache(k, ltd);
//                    this.ltdObjectCleanupList.add(k);
//                }
//            }
//        }catch(Exception e) {
//            log.error("Failed to process metric: {}", updateable, e);
//        }
    }
}

