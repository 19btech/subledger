package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.BaseLtd;
import com.fyntrac.common.entity.InstrumentLevelLtd;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.aggregation.InstrumentLevelAggregationService;
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

public class InstrumentLevelLtdFlatteningWriter implements ItemWriter<List<Records.InstrumentLevelLtdRecord>> {

    private final MongoItemWriter<InstrumentLevelLtd> delegate;
    private final TenantDataSourceProvider dataSourceProvider;
    private final TenantContextHolder tenantContextHolder;
    private final String tenantId;
    private final long jobId;
    private final Map<String, InstrumentLevelLtd> localCache = new HashMap<>();
    private final MemcachedRepository memcachedRepository;
    private final MongoTemplate mongoTemplate;
    private final InstrumentLevelAggregationService instrumentLevelAggregationService;
    private static final int MEMCACHED_TTL_SECONDS = 3600;

    public InstrumentLevelLtdFlatteningWriter(MongoItemWriter<InstrumentLevelLtd> delegate,
                                             TenantDataSourceProvider dataSourceProvider,
                                             TenantContextHolder tenantContextHolder,
                                             MemcachedRepository memcachedRepository,
                                             MongoTemplate mongoTemplate,
                                             InstrumentLevelAggregationService instrumentLevelAggregationService,
                                             String tenantId, long jobId) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
        this.tenantId = tenantId;
        this.jobId = jobId;
        this.memcachedRepository = memcachedRepository;
        this.mongoTemplate = mongoTemplate;
        this.instrumentLevelAggregationService = instrumentLevelAggregationService;
    }

    @Override
    public void write(Chunk<? extends List<Records.InstrumentLevelLtdRecord>> items) throws Exception {



        List<Records.InstrumentLevelLtdRecord> flatRecordList = new ArrayList<>(0);

        for(List<Records.InstrumentLevelLtdRecord> records : items) {
            flatRecordList.addAll(records);
        }

        List<Records.InstrumentLevelLtdRecord> groupedRecords = flatRecordList.stream()
                .collect(Collectors.groupingBy(
                        record -> new GroupKey(
                                record.metricName(),
                                record.instrumentId()
                        ),
                        Collectors.reducing(
                                BigDecimal.ZERO,
                                Records.InstrumentLevelLtdRecord::amount,
                                BigDecimal::add
                        )
                ))
                .entrySet().stream()
                .map(entry -> {
                    GroupKey key = entry.getKey();
                    BigDecimal totalAmount = entry.getValue();

                    // Assuming all records have same postingDate and accountingPeriod
                    // You can safely use flatRecordList.get(0) if the list is not empty
                    Records.InstrumentLevelLtdRecord anyRecord = flatRecordList.get(0);

                    return new Records.InstrumentLevelLtdRecord(
                            key.metricName,
                            key.instrumentId,
                            anyRecord.postingDate(),
                            anyRecord.accountingPeriod(),
                            totalAmount
                    );
                })
                .toList();


        for (Records.InstrumentLevelLtdRecord record : groupedRecords) {
            String key = buildKey(record, tenantId, jobId);

            // Check whether the value was already in localCache
            InstrumentLevelLtd ltd = localCache.get(key);

            if (ltd == null) {
                // Load from Memcached or DB
                ltd = getFromMemcached(key);
                if (ltd == null) {
                    ltd = instrumentLevelAggregationService.getDataService().findOne(buildQuery(record), InstrumentLevelLtd.class);
                }

                if (ltd == null) {
                    // New record → init with previous balance
                    InstrumentLevelLtd previous = instrumentLevelAggregationService.getDataService().findOne(buildPreviousQuery(record), InstrumentLevelLtd.class);
                    BigDecimal prevEnding = previous != null
                            ? previous.getBalance().getEndingBalance()
                            : BigDecimal.ZERO;

                    BaseLtd balance = BaseLtd.builder()
                            .beginningBalance(prevEnding)
                            .activity(record.amount()) // 👈 Already includes current activity
                            .endingBalance(prevEnding.add(record.amount()))
                            .build();

                    ltd = InstrumentLevelLtd.builder()
                            .metricName(record.metricName())
                            .instrumentId(record.instrumentId())
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
        List<InstrumentLevelLtd> ltdChunk = new ArrayList<>();
        for (Map.Entry<String, InstrumentLevelLtd> entry : localCache.entrySet()) {
            ltdChunk.add(entry.getValue());
            putInMemcached(entry.getKey(), entry.getValue());
        }

        Chunk<InstrumentLevelLtd> convertedChunk = this.convertChunk(ltdChunk);

        // ✅ This is the correct call: a Chunk of InstrumentLevelLtd
        String tenant = tenantContextHolder.getTenant();
        if (tenant != null && !tenant.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
            delegate.setTemplate(mongoTemplate);
        }


        delegate.write(convertedChunk);
    }

    private Chunk<InstrumentLevelLtd> convertChunk(List<InstrumentLevelLtd> chunk) {
        List<InstrumentLevelLtd> flattenedList = new ArrayList<>();
        // Iterate through each List<AttributeLevelLtd> in the Chunk
        // Add all elements from the current list to the flattened list
        flattenedList.addAll(chunk);
        // Create a new Chunk<AttributeLevelLtd> from the flattened list

        return new Chunk<>(flattenedList);
    }

    private record GroupKey(String metricName, String instrumentId) implements java.io.Serializable {}

    private String buildKey(Records.InstrumentLevelLtdRecord r, String tenantId, long jobId) {
        return String.format("tenantId:jobId:instrument:ltd:%s:%d:%s:%s:%d",tenantId, jobId, r.metricName(), r.instrumentId(),  r.postingDate());
    }

    private InstrumentLevelLtd getFromMemcached(String key) {
        try {
            return memcachedRepository.getFromCache(key, InstrumentLevelLtd.class);
        } catch (Exception e) {
            return null; // fallback to DB
        }
    }

    private void putInMemcached(String key, InstrumentLevelLtd value) {
        try {
            memcachedRepository.putInCache(key, value, MEMCACHED_TTL_SECONDS);
        } catch (Exception e) {
            // ignore caching failure
        }
    }

    private Query buildQuery(Records.InstrumentLevelLtdRecord r) {
        return new Query(Criteria.where("metricName").is(r.metricName().toUpperCase())
                .and("instrumentId").is(r.instrumentId().toUpperCase())
                .and("postingDate").is(r.postingDate()));
    }

    private Query buildPreviousQuery(Records.InstrumentLevelLtdRecord r) {
        return new Query(Criteria.where("metricName").is(r.metricName().toUpperCase())
                .and("instrumentId").is(r.instrumentId().toUpperCase())
                .and("postingDate").lt(r.postingDate()))
                .with(Sort.by(Sort.Direction.DESC, "postingDate"))
                .limit(1);
    }

}
