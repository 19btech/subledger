package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.repository.AttributesRepository;
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
    private final AttributesRepository attributesRepository;

    String tenantId; // package-private: settable directly by unit tests
    private long runId;
    private AccountingPeriodService accountingPeriodService;
    private long batchId;

    private ExecutionStateService executionStateService;
    private ExecutionState executionState;
    private Long jobId;

    // OPTIMIZATION: Store ReferenceData as a field
    private ReferenceData referenceData;

    // Attribute definition's isVersionable flag, keyed by attributeId (normalised upper-case),
    // preloaded once per step so write() doesn't need a repository round-trip per row.
    // Package-private (not private) so unit tests can populate it directly without going through
    // the full beforeStep()/TenantContextHolder/repository plumbing.
    final Map<String, Boolean> attributeVersionableMap = new HashMap<>();

    private static final int LOCK_TIMEOUT_SECONDS = 10;
    private static final int MAX_RETRIES = 10;
    private static final long RETRY_DELAY_MS = 100;

    public InstrumentAttributeWriter(MongoItemWriter<InstrumentAttribute> delegate,
                                     TenantDataSourceProvider dataSourceProvider,
                                     MemcachedRepository memcachedRepository,
                                     InstrumentAttributeService instrumentAttributeService,
                                     AccountingPeriodService accountingPeriodService,
                                     ExecutionStateService executionStateService,
                                     AttributesRepository attributesRepository) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.memcachedRepository = memcachedRepository;
        this.instrumentAttributeService = instrumentAttributeService;
        this.runId = 0;
        this.accountingPeriodService = accountingPeriodService;
        this.executionStateService = executionStateService;
        this.attributesRepository = attributesRepository;
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

        // Preload each attribute definition's isVersionable flag so write() can decide, per row,
        // whether a value change should open a new version or just overwrite the existing record.
        this.attributeVersionableMap.clear();
        if (this.tenantId != null && !this.tenantId.isEmpty() && this.attributesRepository != null) {
            TenantContextHolder.runWithTenant(this.tenantId, () -> {
                try {
                    attributesRepository.findAll().forEach(attr -> {
                        if (attr.getId() != null) {
                            attributeVersionableMap.put(attr.getId().trim().toUpperCase(), attr.getIsVersionable() != 0);
                        }
                    });
                    log.info("Preloaded {} attribute versionable flags.", attributeVersionableMap.size());
                } catch (Exception e) {
                    log.error("Failed to preload attribute versionable flags for tenant {}", this.tenantId, e);
                }
            });
        }
    }

    /**
     * Whether the attribute definition identified by attributeId is marked versionable
     * (Attributes.isVersionable != 0). Defaults to {@code true} (preserve today's versioning
     * behavior) if the attribute isn't found in the preloaded map — e.g. reference data wasn't
     * loaded yet, or the tenant context was unavailable in beforeStep.
     */
    boolean isAttributeVersionable(String attributeId) {
        if (attributeId == null) {
            return true;
        }
        Boolean versionable = attributeVersionableMap.get(attributeId.trim().toUpperCase());
        return versionable == null || versionable;
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

    /**
     * Builds the version chain for the incoming chunk, grouped by (attributeId, instrumentId).
     *
     * <p>A new version is only opened when the attribute is versionable AND its value actually
     * changed relative to what's already open in the DB:
     * <ul>
     *   <li><b>Not versionable</b> ({@code Attributes.isVersionable == 0}): the existing open
     *       record (if any) is updated in place — same document, no chain, no reclass message.</li>
     *   <li><b>Versionable, value unchanged</b>: the existing open record's window is extended
     *       (postingDate/periodId brought forward) rather than opening a new version.</li>
     *   <li><b>Versionable, value changed</b> (or no existing open record): existing
     *       open/close/reclass-message chaining behavior, unchanged.</li>
     * </ul>
     */
    Chunk<InstrumentAttribute> setEndDate(long batchId, List<InstrumentAttribute> attributesList, CacheList<Records.InstrumentAttributeReclassMessageRecord> localMessages) throws ParseException {

        Map<String, List<InstrumentAttribute>> groupedAttributes = new HashMap<>();
        for (InstrumentAttribute attribute : attributesList) {
            String key = attribute.getAttributeId() + "_" + attribute.getInstrumentId();
            groupedAttributes
                    .computeIfAbsent(key, k -> new ArrayList<>())
                    .add(attribute);
        }

        List<InstrumentAttribute> toWrite = new ArrayList<>();

        for (List<InstrumentAttribute> rawSubChunk : groupedAttributes.values()) {
            List<InstrumentAttribute> sortedSubChunk = rawSubChunk.stream()
                    .sorted(Comparator.comparingInt(InstrumentAttribute::getPeriodId))
                    .collect(Collectors.toList());

            String attributeId = sortedSubChunk.get(0).getAttributeId();
            String instrumentId = sortedSubChunk.get(0).getInstrumentId();

            if (sortedSubChunk.size() == 1) {
                log.info("Processing SINGLE item (Retry Mode) for Instrument: {}", instrumentId);
            }

            List<InstrumentAttribute> openInstrumentAttributes =
                    this.instrumentAttributeService.getOpenInstrumentAttributes(attributeId, instrumentId, this.tenantId);

            if (!isAttributeVersionable(attributeId)) {
                toWrite.addAll(applyNonVersionableUpdate(batchId, sortedSubChunk, openInstrumentAttributes));
                continue;
            }

            // Collapse consecutive rows within this same upload whose value is unchanged, so an
            // unchanged value never opens a spurious new version within the file itself.
            List<InstrumentAttribute> chain = collapseUnchanged(batchId, sortedSubChunk);

            // Resolve the first entry against what's already open in the DB *before* wiring up
            // next-version links below, so any later entry's previousVersionId points at whichever
            // record actually ends up "current" (the new first entry, or the extended existing one).
            InstrumentAttribute first = chain.get(0);
            boolean firstIsNewVersion = true;

            if (sortedSubChunk.size() == 1) {
                log.info("Retry Mode: Found {} open attributes for linking", openInstrumentAttributes.size());
            }

            for (InstrumentAttribute openInstrumentAttribute : openInstrumentAttributes) {
                if (Objects.equals(openInstrumentAttribute.getVersionId(), first.getVersionId())) {
                    continue;
                }

                if (Objects.equals(openInstrumentAttribute.getAttributes(), first.getAttributes())) {
                    // Value unchanged from what's already open — extend that version's window
                    // instead of opening a new one.
                    openInstrumentAttribute.setPostingDate(first.getPostingDate());
                    openInstrumentAttribute.setPeriodId(first.getPeriodId());
                    openInstrumentAttribute.setBatchId(batchId);
                    toWrite.add(openInstrumentAttribute);
                    chain.set(0, openInstrumentAttribute); // later links point at the survivor
                    first = openInstrumentAttribute;
                    firstIsNewVersion = false;
                } else {
                    openInstrumentAttribute.setEndDate(first.getEffectiveDate());
                    openInstrumentAttribute.setCloseDate(DateUtil.convertIntDateToUtc(first.getPostingDate()));
                    first.setPreviousVersionId(openInstrumentAttribute.getVersionId());
                    toWrite.add(openInstrumentAttribute);
                    this.addReclassMessage(batchId, openInstrumentAttribute, first, localMessages);
                }
            }

            for (int i = 0; i < chain.size(); i++) {
                InstrumentAttribute currentAttribute = chain.get(i);

                if (chain.size() > i + 1) {
                    InstrumentAttribute nextAttribute = chain.get(i + 1);
                    currentAttribute.setEndDate(nextAttribute.getEffectiveDate());
                    currentAttribute.setCloseDate(DateUtil.convertIntDateToUtc(nextAttribute.getPostingDate()));
                    nextAttribute.setPreviousVersionId(currentAttribute.getVersionId());
                    this.addReclassMessage(batchId, currentAttribute, nextAttribute, localMessages);
                }

                if (i > 0 || firstIsNewVersion) {
                    toWrite.add(currentAttribute);
                }
            }
        }

        return new Chunk<>(toWrite);
    }

    /**
     * Collapses consecutive rows in the same upload (for one attributeId+instrumentId group)
     * whose attribute values are identical into a single entry, so an unchanged value between two
     * rows in the same file doesn't open a version for the second one — it just extends the
     * first's window (postingDate/periodId brought forward).
     */
    private List<InstrumentAttribute> collapseUnchanged(long batchId, List<InstrumentAttribute> sortedSubChunk) {
        List<InstrumentAttribute> chain = new ArrayList<>();
        for (InstrumentAttribute candidate : sortedSubChunk) {
            if (!chain.isEmpty()) {
                InstrumentAttribute last = chain.get(chain.size() - 1);
                if (Objects.equals(last.getAttributes(), candidate.getAttributes())) {
                    last.setPostingDate(candidate.getPostingDate());
                    last.setPeriodId(candidate.getPeriodId());
                    last.setBatchId(batchId);
                    continue;
                }
            }
            chain.add(candidate);
        }
        return chain;
    }

    /**
     * Non-versionable attribute: no version chain ever — the existing record (if any) is updated
     * in place with the latest incoming row's value; otherwise a single fresh record is created.
     */
    private List<InstrumentAttribute> applyNonVersionableUpdate(long batchId, List<InstrumentAttribute> sortedSubChunk,
                                                                  List<InstrumentAttribute> openInstrumentAttributes) {
        InstrumentAttribute latestIncoming = sortedSubChunk.get(sortedSubChunk.size() - 1);
        if (!openInstrumentAttributes.isEmpty()) {
            InstrumentAttribute existing = openInstrumentAttributes.get(0);
            existing.setAttributes(latestIncoming.getAttributes());
            existing.setEffectiveDate(latestIncoming.getEffectiveDate());
            existing.setPostingDate(latestIncoming.getPostingDate());
            existing.setPeriodId(latestIncoming.getPeriodId());
            existing.setBatchId(batchId);
            log.info("Non-versionable attribute {} for instrument {}: updated existing record in place (no new version).",
                    existing.getAttributeId(), existing.getInstrumentId());
            return List.of(existing);
        }
        return List.of(latestIncoming);
    }

    private void addReclassMessage(long batchId, InstrumentAttribute openInstrumentAttribute, InstrumentAttribute currentAttribute, CacheList<Records.InstrumentAttributeReclassMessageRecord> localMessages) {
        Records.InstrumentAttributeRecord openInstrumentAtt = RecordFactory.createInstrumentAttributeRecord(openInstrumentAttribute);
        Records.InstrumentAttributeRecord currentInstrumentAtt = RecordFactory.createInstrumentAttributeRecord(currentAttribute);
        Records.InstrumentAttributeReclassMessageRecord reclassMessageRecord = RecordFactory.createInstrumentAttributeReclassMessageRecord(tenantId, batchId, openInstrumentAtt, currentInstrumentAtt);

        localMessages.add(reclassMessageRecord);
    }
}