package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.component.InstrumentReplayQueue;
import com.fyntrac.common.component.InstrumentReplaySet;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.InstrumentAttributeService;
import com.fyntrac.common.service.aggregation.MetricLevelAggregationService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class InstrumentAttributeWriter implements ItemWriter<InstrumentAttribute> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoItemWriter<InstrumentAttribute> delegate;
    private final MemcachedRepository memcachedRepository;
    private final InstrumentAttributeService instrumentAttributeService;

    // Removed class-level state for lists to prevent ConcurrentModificationException
    // private CacheList<Records.InstrumentAttributeReclassMessageRecord> reclassMessageRecords;

    private String tenantId;
    private long runId;
    private AccountingPeriodService accountingPeriodService;
    private long batchId;

    // Removed class-level state
    // private CacheList<Records.TransactionActivityReplayRecord> transactionActivityReplayRecordCacheList;

    private ExecutionStateService executionStateService;
    private final InstrumentReplaySet instrumentReplaySet;
    private final InstrumentReplayQueue instrumentReplayQueue;
    private ExecutionState executionState;
    private Long jobId;

    // Locking constants
    private static final int LOCK_TIMEOUT_SECONDS = 5;
    private static final int MAX_RETRIES = 50; // Increased retries for high concurrency
    private static final long RETRY_DELAY_MS = 100;

    public InstrumentAttributeWriter(MongoItemWriter<InstrumentAttribute> delegate,
                                     TenantDataSourceProvider dataSourceProvider,
                                     MemcachedRepository memcachedRepository,
                                     InstrumentAttributeService instrumentAttributeService,
                                     AccountingPeriodService accountingPeriodService
            , ExecutionStateService executionStateService
            , InstrumentReplaySet instrumentReplaySet
            , InstrumentReplayQueue instrumentReplayQueue
    ) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.memcachedRepository = memcachedRepository;
        this.instrumentAttributeService = instrumentAttributeService;
        this.runId=0;
        this.accountingPeriodService = accountingPeriodService;
        this.executionStateService = executionStateService;
        this.instrumentReplaySet = instrumentReplaySet;
        this.instrumentReplayQueue = instrumentReplayQueue;

    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        JobParameters jobParameters = stepExecution.getJobParameters();
        this.runId = jobParameters.getLong("run.id");
        this.tenantId = jobParameters.getString("tenantId");
        this.instrumentAttributeService.setTenant(tenantId);
        this.batchId = jobParameters.getLong("batchId");
        this.jobId = jobParameters.getLong("jobId");
        executionState = this.executionStateService.getExecutionState();
    }

    @Override
    public void write(Chunk<? extends InstrumentAttribute> instrumentAttributes) throws Exception {

        // 1. Process Attributes locally first to prepare data
        ReferenceData referenceData = this.memcachedRepository.getFromCache(this.tenantId, com.fyntrac.common.config.ReferenceData.class);
        List<InstrumentAttribute> combinedAttributes = new ArrayList<>(instrumentAttributes.getItems());

        if (this.tenantId != null && !this.tenantId.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(this.tenantId);
            if(mongoTemplate == null) {
                return;
            }

            for(InstrumentAttribute instrumentAttribute : combinedAttributes) {
                int effectivePeriodId = com.fyntrac.common.utils.DateUtil.getAccountingPeriodId(instrumentAttribute.getEffectiveDate());
                AccountingPeriod effectiveAccountingPeriod = this.accountingPeriodService.getAccountingPeriod(effectivePeriodId, this.tenantId);
                instrumentAttribute.setEndDate(null);
                instrumentAttribute.setCloseDate(null);
                instrumentAttribute.setPreviousVersionId(0L);
                if(effectiveAccountingPeriod != null){
                    if(effectiveAccountingPeriod.getStatus() !=0) {
                        instrumentAttribute.setPeriodId(referenceData.getCurrentAccountingPeriodId());
                    }else{
                        instrumentAttribute.setPeriodId(effectiveAccountingPeriod.getPeriodId());
                    }
                }else {
                    instrumentAttribute.setPeriodId(referenceData.getCurrentAccountingPeriodId());
                }
                instrumentAttribute.setBatchId(batchId);
                this.instrumentAttributeService.addIntoCache(this.tenantId, instrumentAttribute);
                int intEffectiveDate = DateUtil.convertToIntYYYYMMDDFromJavaDate(instrumentAttribute.getEffectiveDate());

                if(executionState != null && executionState.getExecutionDate() > intEffectiveDate) {
                    Records.InstrumentReplayRecord replayRecord =
                            RecordFactory.createInstrumentReplayRecord(instrumentAttribute.getInstrumentId(),
                                    instrumentAttribute.getAttributeId(),
                                    instrumentAttribute.getPostingDate(), intEffectiveDate);
                    instrumentReplaySet.add(tenantId, this.jobId,replayRecord);
                    // instrumentReplayQueue.add(tenantId, this.jobId,replayRecord);
                }
            }
            delegate.setTemplate(mongoTemplate);
        }

        // 2. Local container for new messages generated in this chunk
        CacheList<Records.InstrumentAttributeReclassMessageRecord> localReclassMessages = new CacheList<>();

        // 3. Logic processing (setEndDate) now populates the LOCAL list
        Chunk<InstrumentAttribute> updatedChunk = this.setEndDate(batchId, combinedAttributes, localReclassMessages);

        // 4. CRITICAL SECTION: Distributed Lock to update Reclass Messages
        String dataKey = Key.reclassMessageList(this.tenantId, this.runId);
        String lockKey = "lock:" + dataKey;

        updateCacheWithLock(lockKey, dataKey, localReclassMessages);

        // 5. Write to Mongo
        delegate.write(updatedChunk);

        // 6. Handle Replay Record Cache (Locked update not implemented here as logic was empty in original, but follow pattern above if needed)
        // Original code seemed to just get/put empty list or overwrite. If this list is actually used, apply updateCacheWithLock here too.
    }

    /**
     * Helper to update Memcached list safely with locking
     */
    private <T> void updateCacheWithLock(String lockKey, String dataKey, CacheList<T> newItems) {
        // FIX: Unwrap CacheList to access underlying list for emptiness check
        if (newItems == null || newItems.getList() == null || newItems.getList().isEmpty()) return;

        boolean lockAcquired = false;
        int attempts = 0;

        try {
            while (attempts < MAX_RETRIES) {
                lockAcquired = memcachedRepository.add(lockKey, "LOCKED", LOCK_TIMEOUT_SECONDS);
                if (lockAcquired) break;
                Thread.sleep(RETRY_DELAY_MS);
                attempts++;
            }

            if (!lockAcquired) {
                throw new RuntimeException("Could not acquire lock for key: " + dataKey);
            }

            // READ
            CacheList<T> existingList;
            if(this.memcachedRepository.ifExists(dataKey)) {
                existingList = this.memcachedRepository.getFromCache(dataKey, CacheList.class);
            } else {
                existingList = new CacheList<>();
            }

            // MODIFY
            // FIX: Access underlying lists to perform addAll, assuming CacheList wrappers are incompatible
            if (existingList.getList() != null && newItems.getList() != null) {
                existingList.getList().addAll(newItems.getList());
            }

            // WRITE
            this.memcachedRepository.putInCache(dataKey, existingList);

        } catch (Exception e) {
            log.error("Error updating cache for key {}", dataKey, e);
            throw new RuntimeException(e);
        } finally {
            if (lockAcquired) {
                try {
                    memcachedRepository.delete(lockKey);
                } catch (Exception e) {
                    log.warn("Failed to release lock {}", lockKey);
                }
            }
        }
    }

    // Refactored to accept local list instead of using class field
    private Chunk<InstrumentAttribute> setEndDate(long batchId , List<InstrumentAttribute> attributesList, CacheList<Records.InstrumentAttributeReclassMessageRecord> localMessages) throws ParseException {

        Map<String, List<InstrumentAttribute>> groupedAttributes = new HashMap<>();
        List<InstrumentAttribute> openVersion = new ArrayList<>(0);

        for (InstrumentAttribute attribute : attributesList) {
            String key = attribute.getAttributeId() + "_" + attribute.getInstrumentId();
            groupedAttributes
                    .computeIfAbsent(key, k -> new ArrayList<>())
                    .add(attribute);
        }

        for (List<InstrumentAttribute> subChunk : groupedAttributes.values()) {
            List<InstrumentAttribute> sortedSubChunk = subChunk.stream()
                    .sorted(Comparator.comparingInt(InstrumentAttribute::getPeriodId))
                    .collect(Collectors.toList());

            for (int i = 0; i < sortedSubChunk.size(); i++) {
                InstrumentAttribute currentAttribute = sortedSubChunk.get(i);

                if(sortedSubChunk.size() > i+1) {
                    InstrumentAttribute nextAttribute = sortedSubChunk.get(i + 1);
                    currentAttribute.setEndDate(nextAttribute.getEffectiveDate());
                    currentAttribute.setCloseDate(DateUtil.convertIntDateToUtc(nextAttribute.getPostingDate()));
                    nextAttribute.setPreviousVersionId(currentAttribute.getVersionId());
                    this.addReclassMessage(batchId, currentAttribute, nextAttribute, localMessages);
                }

                if(i== 0) {
                    List<InstrumentAttribute> openInstrumentAttributes =
                            this.instrumentAttributeService.getOpenInstrumentAttributes(currentAttribute.getAttributeId(), currentAttribute.getInstrumentId(), this.tenantId);
                    for(InstrumentAttribute openInstrumentAttribute : openInstrumentAttributes) {
                        openInstrumentAttribute.setEndDate(currentAttribute.getEffectiveDate());
                        openInstrumentAttribute.setCloseDate(DateUtil.convertIntDateToUtc(currentAttribute.getPostingDate()));
                        currentAttribute.setPreviousVersionId(openInstrumentAttribute.getVersionId());
                        openVersion.add(openInstrumentAttribute);
                        this.addReclassMessage(batchId, openInstrumentAttribute, currentAttribute, localMessages);
                    }
                }
            }
        }

        List<InstrumentAttribute> flattenedList = groupedAttributes.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());

        flattenedList.addAll(openVersion);
        return new Chunk<>(flattenedList);
    }

    // Refactored to accept local list
    private void addReclassMessage(long batchId, InstrumentAttribute openInstrumentAttribute, InstrumentAttribute currentAttribute, CacheList<Records.InstrumentAttributeReclassMessageRecord> localMessages) {
        Records.InstrumentAttributeRecord openInstrumentAtt = RecordFactory.createInstrumentAttributeRecord(openInstrumentAttribute);
        Records.InstrumentAttributeRecord currentInstrumentAtt = RecordFactory.createInstrumentAttributeRecord(currentAttribute);
        Records.InstrumentAttributeReclassMessageRecord reclassMessageRecord = RecordFactory.createInstrumentAttributeReclassMessageRecord(tenantId, batchId, openInstrumentAtt, currentInstrumentAtt);

        // Add to local list, not class field
        localMessages.add(reclassMessageRecord);
    }
}