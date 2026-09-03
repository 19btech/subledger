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

    // Attribute definition's isVersionable flag, keyed by Attributes.attributeName (normalised
    // upper-case) — NOT by InstrumentAttribute.attributeId. attributeId is an opaque business/
    // grouping key on the InstrumentAttribute document (same role it plays on AttributeLevelLtd /
    // TransactionActivity); the actual attribute definitions (e.g. "ORDER_DATE") live as *keys
    // inside the attributes map* on each document, exactly like TransactionActivityService
    // .getReclassableAttributes(Map) already cross-references isReclassable by attributeName.
    // Preloaded once per step so write() doesn't need a repository round-trip per row.
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
                        if (attr.getAttributeName() != null) {
                            attributeVersionableMap.put(attr.getAttributeName().trim().toUpperCase(), attr.getIsVersionable() != 0);
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
     * Whether the attribute definition named attributeName (a key inside InstrumentAttribute
     * .attributes) is marked versionable (Attributes.isVersionable != 0). Defaults to
     * {@code true} (preserve today's versioning behavior) if the name isn't found in the
     * preloaded map — e.g. reference data wasn't loaded yet, the tenant context was unavailable
     * in beforeStep, or the upload references an attribute name with no matching Attributes
     * definition.
     */
    boolean isAttributeNameVersionable(String attributeName) {
        if (attributeName == null) {
            return true;
        }
        Boolean versionable = attributeVersionableMap.get(attributeName.trim().toUpperCase());
        return versionable == null || versionable;
    }

    /**
     * Whether at least one field carried in these rows' attributes maps is versionable. When
     * none are (every field present is explicitly Attributes.isVersionable == 0), the whole
     * attributeId/instrumentId group is a plain in-place update — nothing about it should ever
     * open a version, regardless of what changed.
     */
    private boolean hasVersionableAttribute(List<InstrumentAttribute> rows) {
        boolean sawAnyKey = false;
        for (InstrumentAttribute row : rows) {
            Map<String, Object> attrs = row.getAttributes();
            if (attrs == null) {
                continue;
            }
            for (String key : attrs.keySet()) {
                sawAnyKey = true;
                if (isAttributeNameVersionable(key)) {
                    return true;
                }
            }
        }
        return !sawAnyKey; // no fields at all to inspect -> preserve the old always-version default
    }

    /**
     * Projects an attributes map down to just the entries whose attribute definition is
     * versionable, so the "did the value change" comparisons below only look at fields that
     * should actually drive a new version — a non-versionable field changing alongside doesn't
     * spuriously trigger (or block) one.
     */
    private Map<String, Object> versionableProjection(Map<String, Object> attributes) {
        if (attributes == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> projection = new HashMap<>();
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (isAttributeNameVersionable(entry.getKey())) {
                projection.put(entry.getKey(), entry.getValue());
            }
        }
        return projection;
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
     * <p>Each document's {@code attributes} map can carry several attribute-definition fields at
     * once (e.g. ORDER_DATE alongside others), each independently flagged versionable or not in
     * the {@code Attributes} collection (keyed by attributeName — see {@link #attributeVersionableMap}).
     * A new version is only opened when at least one <em>versionable</em> field's value actually
     * changed relative to what's already open in the DB:
     * <ul>
     *   <li><b>Rows sharing the same postingDate AND effectiveDate</b> are folded into a single
     *       row before any diffing happens (see {@link #mergeSameDateRows}) — one upload can carry
     *       several attribute-field changes for the same date, and those changes together make up
     *       exactly one version, never one version per row.</li>
     *   <li><b>No versionable fields present</b> (every field on the row is explicitly
     *       {@code Attributes.isVersionable == 0}): the existing open record (if any) is updated
     *       in place — same document, no chain, no reclass message.</li>
     *   <li><b>Versionable fields unchanged</b>: the existing open record is left as-is
     *       (postingDate/periodId untouched — no new version is opened), even if a
     *       non-versionable field alongside them changed.</li>
     *   <li><b>A versionable field changed</b> (or no existing open record): existing
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

            // One or more rows in this upload sharing the same postingDate AND effectiveDate
            // represent the same version's data split across multiple lines (e.g. one attribute
            // field per line) — fold them into a single row before diffing, so they produce
            // exactly one version instead of one per row.
            sortedSubChunk = mergeSameDateRows(sortedSubChunk);

            List<InstrumentAttribute> openInstrumentAttributes =
                    this.instrumentAttributeService.getOpenInstrumentAttributes(attributeId, instrumentId, this.tenantId);

            if (!hasVersionableAttribute(sortedSubChunk)) {
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

                if (Objects.equals(versionableProjection(openInstrumentAttribute.getAttributes()), versionableProjection(first.getAttributes()))) {
                    // Versionable fields unchanged from what's already open — keep that version
                    // as-is (postingDate/periodId untouched) rather than opening a new one, even
                    // if a non-versionable field alongside it changed. Only batchId is stamped,
                    // to record that this batch touched/confirmed the record.
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
     * Folds rows within this attributeId+instrumentId group that share the same postingDate AND
     * effectiveDate into a single row, combining their attribute maps. An upload can carry several
     * lines for the same instrument/date — e.g. one attribute field changed per line — and those
     * lines are all part of the same version, not one version each. Rows are merged in upload
     * order: the first row seen for a given (postingDate, effectiveDate) pair is kept as the
     * surviving record (its id/versionId/etc. are preserved), and later rows for that same pair
     * only contribute their attribute entries into it (a later row's key wins on conflict).
     */
    private List<InstrumentAttribute> mergeSameDateRows(List<InstrumentAttribute> sortedSubChunk) {
        Map<String, InstrumentAttribute> mergedByDate = new LinkedHashMap<>();
        for (InstrumentAttribute row : sortedSubChunk) {
            String dateKey = row.getPostingDate() + "_" + (row.getEffectiveDate() == null ? "null" : row.getEffectiveDate().getTime());
            InstrumentAttribute merged = mergedByDate.get(dateKey);
            if (merged == null) {
                // Defensive copy: this row survives as the merged record, so give it its own
                // attributes map rather than mutating whatever the caller handed in.
                Map<String, Object> attrsCopy = new HashMap<>();
                if (row.getAttributes() != null) {
                    attrsCopy.putAll(row.getAttributes());
                }
                row.setAttributes(attrsCopy);
                mergedByDate.put(dateKey, row);
            } else if (row.getAttributes() != null) {
                merged.getAttributes().putAll(row.getAttributes());
            }
        }
        return new ArrayList<>(mergedByDate.values());
    }

    /**
     * Collapses consecutive rows in the same upload (for one attributeId+instrumentId group)
     * whose versionable attribute values are identical into a single entry, so an unchanged
     * value between two rows in the same file doesn't open a version for the second one — the
     * surviving row's postingDate/periodId are left as they were; the discarded row's dates are
     * not carried forward.
     */
    private List<InstrumentAttribute> collapseUnchanged(long batchId, List<InstrumentAttribute> sortedSubChunk) {
        List<InstrumentAttribute> chain = new ArrayList<>();
        for (InstrumentAttribute candidate : sortedSubChunk) {
            if (!chain.isEmpty()) {
                InstrumentAttribute last = chain.get(chain.size() - 1);
                if (Objects.equals(versionableProjection(last.getAttributes()), versionableProjection(candidate.getAttributes()))) {
                    // Unchanged value within the same upload — keep the surviving row's
                    // postingDate/periodId as-is; the later candidate's dates are discarded.
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
