package com.reserv.dataloader.service;

import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.BaseLtd;
import com.fyntrac.common.entity.MetricLevelLtd;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Metric-level LTD for one DSL run, computed once after every batch has finished.
 *
 * <p>This used to run per batch (metricLevelLtdJob, once per ~300-instrument batch) and read-modify-write
 * the same few MetricLevelLtd rows — keyed by (metric, postingDate), no instrument — so it had to be
 * serialized behind a lock in DslExecutionWorkflow: ~240 locked job launches per Hearst posting date,
 * ~16,000 at 8M instruments, and a hard blocker for running batches on more than one JVM.
 *
 * <p>Here it is a computation instead of an accumulation: sum this run's TransactionActivity amounts
 * (Decimal128, so exact) per transaction, map transactions to metrics, and write each row once. The
 * arithmetic is the per-batch writer's (MetricLevelLtdFlatteningWriter) applied to the whole run: an
 * existing row for (metric, postingDate) gets the activity added; otherwise the row starts from the
 * latest earlier posting date's ending balance.
 */
@Slf4j
@Service
public class MetricLevelRollupService {

    private final AggregationService aggregationService;
    private final MemcachedRepository memcachedRepository;
    private final TenantDataSourceProvider dataSourceProvider;

    public MetricLevelRollupService(AggregationService aggregationService,
                                    MemcachedRepository memcachedRepository,
                                    TenantDataSourceProvider dataSourceProvider) {
        this.aggregationService = aggregationService;
        this.memcachedRepository = memcachedRepository;
        this.dataSourceProvider = dataSourceProvider;
    }

    /**
     * @param jobIds the model batches of this run whose transactions the per-batch metric step would
     *               have aggregated (batches whose instrument-scoped aggregation succeeded)
     */
    public void rollUp(String tenant, int postingDate, Collection<Long> jobIds) {
        if (jobIds == null || jobIds.isEmpty()) {
            log.info("Metric roll-up: no batches to roll up for tenant {} postingDate {}", tenant, postingDate);
            return;
        }
        long start = System.currentTimeMillis();
        MongoTemplate mongo = dataSourceProvider.getDataSource(tenant);

        // Same transaction -> metrics map the per-batch step used (MetricLevelLtdBatchConfig).
        aggregationService.setTenant(tenant);
        aggregationService.loadIntoCache();
        CacheMap<Set<String>> metricCache = memcachedRepository.getFromCache(Key.allMetricList(tenant), CacheMap.class);
        Map<String, Set<String>> transactionToMetrics = metricCache == null ? Map.of() : metricCache.getMap();

        Aggregation totalsByTransaction = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("postingDate").is(postingDate)
                        .and("batchId").in(jobIds)
                        .and("transactionName").ne(null)),
                Aggregation.group("transactionName").sum("amount").as("total"));

        Map<String, BigDecimal> activityByMetric = new LinkedHashMap<>();
        for (Document row : mongo.aggregate(totalsByTransaction, "TransactionActivity", Document.class)) {
            String transactionName = row.getString("_id");
            BigDecimal total = toBigDecimal(row.get("total"));
            for (String metric : transactionToMetrics.getOrDefault(transactionName.toUpperCase(), Set.of())) {
                activityByMetric.merge(metric, total, BigDecimal::add);
            }
        }

        for (Map.Entry<String, BigDecimal> entry : activityByMetric.entrySet()) {
            String metric = entry.getKey();
            BigDecimal activity = entry.getValue();

            MetricLevelLtd ltd = mongo.findOne(new Query(Criteria.where("postingDate").is(postingDate)
                    .and("metricName").is(metric.toUpperCase())), MetricLevelLtd.class);
            if (ltd == null) {
                MetricLevelLtd previous = mongo.findOne(new Query(Criteria.where("metricName").is(metric.toUpperCase())
                        .and("postingDate").lt(postingDate))
                        .with(Sort.by(Sort.Direction.DESC, "postingDate"))
                        .limit(1), MetricLevelLtd.class);
                BigDecimal previousEnding = previous != null ? previous.getBalance().getEndingBalance() : BigDecimal.ZERO;
                ltd = MetricLevelLtd.builder()
                        .metricName(metric)
                        .postingDate(postingDate)
                        .accountingPeriodId(DateUtil.getAccountingPeriodId(postingDate))
                        .balance(BaseLtd.builder()
                                .beginningBalance(previousEnding)
                                .activity(activity)
                                .endingBalance(previousEnding.add(activity))
                                .build())
                        .build();
            } else {
                BaseLtd balance = ltd.getBalance();
                BigDecimal updatedActivity = balance.getActivity().add(activity);
                balance.setActivity(updatedActivity);
                balance.setEndingBalance(balance.getBeginningBalance().add(updatedActivity));
            }
            mongo.save(ltd);
        }

        log.info("Metric roll-up: tenant {} postingDate {} — {} batches, {} metrics in {} ms",
                tenant, postingDate, jobIds.size(), activityByMetric.size(), System.currentTimeMillis() - start);
    }

    private static BigDecimal toBigDecimal(Object value) {
        if (value instanceof Decimal128 decimal) {
            return decimal.bigDecimalValue();
        }
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        return value == null ? BigDecimal.ZERO : new BigDecimal(value.toString());
    }
}
