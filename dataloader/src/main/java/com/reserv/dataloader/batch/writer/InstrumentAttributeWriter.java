package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.cache.collection.CacheList;
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
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
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

    private String tenantId;
    private long runId;
    private AccountingPeriodService accountingPeriodService;
    private long batchId;

    private ExecutionStateService executionStateService;
    private ExecutionState executionState;
    private Long jobId;

    // OPTIMIZATION: Store ReferenceData as a field
    private ReferenceData referenceData;

    private static final int LOCK_TIMEOUT_SECONDS = 10;
    private static final int MAX_RETRIES = 10;
    private static final long RETRY_DELAY_MS = 100;

    public InstrumentAttributeWriter(MongoItemWriter<InstrumentAttribute> delegate,
                                     TenantDataSourceProvider dataSourceProvider,
                                     MemcachedRepository memcachedRepository,
                                     InstrumentAttributeService instrumentAttributeService,
                                     AccountingPeriodService accountingPeriodService,
                                     ExecutionStateService executionStateService) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.memcachedRepository = memcachedRepository;
        this.instrumentAttributeService = instrumentAttributeService;
        this.runId = 0;
        this.accountingPeriodService = accountingPeriodService;
        this.executionStateService = executionStateService;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        JobParameters jobParameters = stepExecution.getJobParameters();
        this.runId = jobParameters.getLong("run.id");
        this.tenantId = jobParameters.getString("tenantId");
        this.instrumentAttributeService.setTenant(tenantId);
        this.batchId = jobParameters.getLong("batchId");
        this.jobId = jobParameters.getLong("jobId");
        this.executionState = this.executionStateService.getExecutionState();

        // FIX: Fetch ReferenceData ONCE here, not in every write call
        try {
            this.referenceData = this.memcachedRepository.getFromCache(
                    this.tenantId,
                    com.fyntrac.common.config.ReferenceData.class
            );

            if (this.referenceData == null) {
                log.warn("ReferenceData is null for tenant: {}", this.tenantId);
                // Handle null case appropriately if needed, or throw exception to fail fast
            }
        } catch (Exception e) {
            log.error("Failed to fetch ReferenceData during BeforeStep", e);
            throw new RuntimeException("Could not initialize writer due to Cache Error", e);
        }
    }

    @Override
    public void write(Chunk<? extends InstrumentAttribute> instrumentAttributes) throws Exception {
        // REMOVED: The expensive cache call that was causing the timeout

        List<InstrumentAttribute> combinedAttributes = new ArrayList<>(instrumentAttributes.getItems());

        if (this.tenantId != null && !this.tenantId.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(this.tenantId);
            if (mongoTemplate == null) {
                return;
            }

            for (InstrumentAttribute instrumentAttribute : combinedAttributes) {

                if (instrumentAttribute.getId() == null) {
                    instrumentAttribute.setId(new ObjectId().toString());
                }

                int effectivePeriodId = com.fyntrac.common.utils.DateUtil.getAccountingPeriodId(instrumentAttribute.getEffectiveDate());
                AccountingPeriod effectiveAccountingPeriod = this.accountingPeriodService.getAccountingPeriod(effectivePeriodId, this.tenantId);

                instrumentAttribute.setEndDate(null);
                instrumentAttribute.setCloseDate(null);
                instrumentAttribute.setPreviousVersionId(0L);

                // Use the cached referenceData field
                if (effectiveAccountingPeriod != null) {
                    if (effectiveAccountingPeriod.getStatus() != 0) {
                        instrumentAttribute.setPeriodId(this.referenceData.getCurrentAccountingPeriodId());
                    } else {
                        instrumentAttribute.setPeriodId(effectiveAccountingPeriod.getPeriodId());
                    }
                } else {
                    instrumentAttribute.setPeriodId(this.referenceData.getCurrentAccountingPeriodId());
                }

                instrumentAttribute.setBatchId(batchId);

                // Warning: Calling cache put in a loop is still risky for high volume.
                // If possible, verify if instrumentAttributeService supports bulk additions.
                this.instrumentAttributeService.addIntoCache(this.tenantId, instrumentAttribute);
            }
            delegate.setTemplate(mongoTemplate);
        }

        CacheList<Records.InstrumentAttributeReclassMessageRecord> localReclassMessages = new CacheList<>();

        // Logic processing
        Chunk<InstrumentAttribute> updatedChunk = this.setEndDate(batchId, combinedAttributes, localReclassMessages);

        String dataKey = Key.reclassMessageList(this.tenantId, this.runId);
        String lockKey = "lock:" + dataKey;

        updateCacheWithLock(lockKey, dataKey, localReclassMessages);

        delegate.write(updatedChunk);
    }

    // ... (Rest of your methods remain unchanged) ...

    private <T> void updateCacheWithLock(String lockKey, String dataKey, CacheList<T> newItems) {
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

            CacheList<T> existingList;
            if (this.memcachedRepository.ifExists(dataKey)) {
                existingList = this.memcachedRepository.getFromCache(dataKey, CacheList.class);
            } else {
                existingList = new CacheList<>();
            }

            if (existingList.getList() == null) {
                existingList.addAll(new ArrayList<>());
            }

            if (newItems.getList() != null) {
                existingList.getList().addAll(newItems.getList());
            }

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

    private Chunk<InstrumentAttribute> setEndDate(long batchId, List<InstrumentAttribute> attributesList, CacheList<Records.InstrumentAttributeReclassMessageRecord> localMessages) throws ParseException {

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

            if (sortedSubChunk.size() == 1) {
                log.info("Processing SINGLE item (Retry Mode) for Instrument: {}", sortedSubChunk.get(0).getInstrumentId());
            }

            for (int i = 0; i < sortedSubChunk.size(); i++) {
                InstrumentAttribute currentAttribute = sortedSubChunk.get(i);

                if (sortedSubChunk.size() > i + 1) {
                    InstrumentAttribute nextAttribute = sortedSubChunk.get(i + 1);
                    currentAttribute.setEndDate(nextAttribute.getEffectiveDate());
                    currentAttribute.setCloseDate(DateUtil.convertIntDateToUtc(nextAttribute.getPostingDate()));
                    nextAttribute.setPreviousVersionId(currentAttribute.getVersionId());
                    this.addReclassMessage(batchId, currentAttribute, nextAttribute, localMessages);
                }

                if (i == 0) {
                    List<InstrumentAttribute> openInstrumentAttributes =
                            this.instrumentAttributeService.getOpenInstrumentAttributes(currentAttribute.getAttributeId(), currentAttribute.getInstrumentId(), this.tenantId);

                    if (sortedSubChunk.size() == 1) {
                        log.info("Retry Mode: Found {} open attributes for linking", openInstrumentAttributes.size());
                    }

                    for (InstrumentAttribute openInstrumentAttribute : openInstrumentAttributes) {
                        if (Objects.equals(openInstrumentAttribute.getVersionId(), currentAttribute.getVersionId())) {
                            continue;
                        }
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

    private void addReclassMessage(long batchId, InstrumentAttribute openInstrumentAttribute, InstrumentAttribute currentAttribute, CacheList<Records.InstrumentAttributeReclassMessageRecord> localMessages) {
        Records.InstrumentAttributeRecord openInstrumentAtt = RecordFactory.createInstrumentAttributeRecord(openInstrumentAttribute);
        Records.InstrumentAttributeRecord currentInstrumentAtt = RecordFactory.createInstrumentAttributeRecord(currentAttribute);
        Records.InstrumentAttributeReclassMessageRecord reclassMessageRecord = RecordFactory.createInstrumentAttributeReclassMessageRecord(tenantId, batchId, openInstrumentAtt, currentInstrumentAtt);

        localMessages.add(reclassMessageRecord);
    }
}