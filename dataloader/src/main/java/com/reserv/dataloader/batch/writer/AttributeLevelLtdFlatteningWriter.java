package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.AttributeLevelLtd;
import com.fyntrac.common.entity.BaseLtd;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.aggregation.AttributeLevelAggregationService;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AttributeLevelLtdFlatteningWriter implements ItemWriter<List<Records.AttributeLevelLtdRecord>> {

    private final MongoItemWriter<AttributeLevelLtd> delegate;
    private final TenantDataSourceProvider dataSourceProvider;
    private final TenantContextHolder tenantContextHolder;
    private final AttributeLevelAggregationService attributeLevelAggregationService;
    private final String tenantId;
    private final long jobId;
    private final Map<String, AttributeLevelLtd> localCache = new HashMap<>();
    private final MemcachedRepository memcachedRepository;
    private final MongoTemplate mongoTemplate;
    private static final int MEMCACHED_TTL_SECONDS = 3600;

    public AttributeLevelLtdFlatteningWriter(MongoItemWriter<AttributeLevelLtd> delegate,
                                             TenantDataSourceProvider dataSourceProvider,
                                             TenantContextHolder tenantContextHolder,
                                             MemcachedRepository memcachedRepository,
                                             AttributeLevelAggregationService attributeLevelAggregationService,
                                             MongoTemplate mongoTemplate,
                                             String tenantId, long jobId) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
        this.tenantId = tenantId;
        this.jobId = jobId;
        this.memcachedRepository = memcachedRepository;
        this.mongoTemplate = mongoTemplate;
        this.attributeLevelAggregationService = attributeLevelAggregationService;
    }

    @Override
    public void write(Chunk<? extends List<Records.AttributeLevelLtdRecord>> items) throws Exception {



        List<Records.AttributeLevelLtdRecord> flatRecordList = new ArrayList<>(0);

        for(List<Records.AttributeLevelLtdRecord> records : items) {
            flatRecordList.addAll(records);
        }

        List<Records.AttributeLevelLtdRecord> groupedRecords = flatRecordList.stream()
                .collect(Collectors.groupingBy(
                        record -> new GroupKey(
                                record.metricName(),
                                record.instrumentId(),
                                record.attributeId()
                        ),
                        Collectors.reducing(
                                BigDecimal.ZERO,
                                Records.AttributeLevelLtdRecord::amount,
                                BigDecimal::add
                        )
                ))
                .entrySet().stream()
                .map(entry -> {
                    GroupKey key = entry.getKey();
                    BigDecimal totalAmount = entry.getValue();

                    // Assuming all records have same postingDate and accountingPeriod
                    // You can safely use flatRecordList.get(0) if the list is not empty
                    Records.AttributeLevelLtdRecord anyRecord = flatRecordList.get(0);

                    return new Records.AttributeLevelLtdRecord(
                            key.metricName,
                            key.instrumentId,
                            key.attributeId,
                            anyRecord.postingDate(),
                            anyRecord.accountingPeriod(),
                            totalAmount
                    );
                })
                .toList();

        for (Records.AttributeLevelLtdRecord record : groupedRecords) {
            String key = buildKey(record, tenantId, jobId);

            // Check whether the value was already in localCache
            AttributeLevelLtd ltd = localCache.get(key);

            if (ltd == null) {
                // Load from Memcached or DB
                ltd = getFromMemcached(key);
                if (ltd == null) {
                    ltd = attributeLevelAggregationService.getDataService().findOne(buildQuery(record), AttributeLevelLtd.class);
                }

                if (ltd == null) {
                    // New record → init with previous balance
                    AttributeLevelLtd previous = attributeLevelAggregationService.getDataService().findOne(buildPreviousQuery(record), AttributeLevelLtd.class);
                    BigDecimal prevEnding = previous != null
                            ? previous.getBalance().getEndingBalance()
                            : BigDecimal.ZERO;

                    BaseLtd balance = BaseLtd.builder()
                            .beginningBalance(prevEnding)
                            .activity(record.amount()) // 👈 Already includes current activity
                            .endingBalance(prevEnding.add(record.amount()))
                            .build();

                    ltd = AttributeLevelLtd.builder()
                            .metricName(record.metricName())
                            .instrumentId(record.instrumentId())
                            .attributeId(record.attributeId())
                            .postingDate(record.postingDate())
                            .accountingPeriodId(record.accountingPeriod())
                            .balance(balance)
                            .build();
                } else {
                    // Found in DB or cache → add activity
                    BaseLtd bal = ltd.getBalance();
                    BigDecimal updatedActivity = bal.getActivity().add(record.amount());
                    bal.setActivity(updatedActivity);
                    bal.setEndingBalance(bal.getBeginningBalance().add(updatedActivity));
                }

                localCache.put(key, ltd);
            } else {
                // Cached in localCache → just update activity
                BaseLtd bal = ltd.getBalance();
                BigDecimal updatedActivity = bal.getActivity().add(record.amount());
                bal.setActivity(updatedActivity);
                bal.setEndingBalance(bal.getBeginningBalance().add(updatedActivity));
            }
        }

// Collect and cache
        List<AttributeLevelLtd> ltdChunk = new ArrayList<>();

        for (Map.Entry<String, AttributeLevelLtd> entry : localCache.entrySet()) {
            ltdChunk.add(entry.getValue());
            putInMemcached(entry.getKey(), entry.getValue());
        }

        Chunk<AttributeLevelLtd> convertedChunk = this.convertChunk(ltdChunk);

        // ✅ This is the correct call: a Chunk of AttributeLevelLtd
        String tenant = tenantContextHolder.getTenant();
        if (tenant != null && !tenant.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
            delegate.setTemplate(mongoTemplate);
        }


        delegate.write(convertedChunk);
    }

    private Chunk<AttributeLevelLtd> convertChunk(List<AttributeLevelLtd> chunk) {
        List<AttributeLevelLtd> flattenedList = new ArrayList<>();
        // Iterate through each List<AttributeLevelLtd> in the Chunk
            // Add all elements from the current list to the flattened list
            flattenedList.addAll(chunk);
        // Create a new Chunk<AttributeLevelLtd> from the flattened list

        return new Chunk<>(flattenedList);
    }

    private record GroupKey(String metricName, String instrumentId, String attributeId) implements java.io.Serializable {}

    private String buildKey(Records.AttributeLevelLtdRecord r, String tenantId, long jobId) {
        return String.format("tenantId:jobId:attr:ltd:%s:%d:%s:%s:%s:%d",tenantId, jobId, r.metricName(), r.instrumentId(), r.attributeId(), r.postingDate());
    }

    private AttributeLevelLtd getFromMemcached(String key) {
        try {
            return memcachedRepository.getFromCache(key, AttributeLevelLtd.class);
        } catch (Exception e) {
            return null; // fallback to DB
        }
    }

    private void putInMemcached(String key, AttributeLevelLtd value) {
        try {
            memcachedRepository.putInCache(key, value, MEMCACHED_TTL_SECONDS);
        } catch (Exception e) {
            // ignore caching failure
        }
    }

    private Query buildQuery(Records.AttributeLevelLtdRecord r) {
        return new Query(Criteria.where("metricName").is(r.metricName().toUpperCase())
                .and("instrumentId").is(r.instrumentId().toUpperCase())
                .and("attributeId").is(r.attributeId().toUpperCase())
                .and("postingDate").is(r.postingDate()));
    }

    private Query buildPreviousQuery(Records.AttributeLevelLtdRecord r) {
        return new Query(Criteria.where("metricName").is(r.metricName().toUpperCase())
                .and("instrumentId").is(r.instrumentId().toUpperCase())
                .and("attributeId").is(r.attributeId().toUpperCase())
                .and("postingDate").lt(r.postingDate()))
                .with(Sort.by(Sort.Direction.DESC, "postingDate"))
                .limit(1);
    }

}
