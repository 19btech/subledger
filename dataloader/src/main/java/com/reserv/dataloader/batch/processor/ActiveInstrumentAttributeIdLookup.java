package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.repository.InstrumentAttributeRepository;
import com.reserv.dataloader.validation.TransactionActivityValidator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Answers "does an ACTIVE InstrumentAttribute row exist with this instrumentId / attributeId?"
 * for the rows of one Spring Batch chunk with one query per ID field, instead of preloading the
 * whole collection into heap once per step.
 *
 * <p>Spring Batch reads every item of a chunk before it processes the first one, so a processor
 * that is also registered as an {@code ItemReadListener} can {@link #collect} each row's IDs in
 * {@code afterRead}, and the first {@link #ensureChecked} of the chunk resolves them all at once.
 * The result sets are replaced on every flush, so heap usage is bounded by the chunk size.
 *
 * <p>Matching semantics are the same as the preload this replaces: both sides are compared after
 * {@link TransactionActivityValidator#normalizeId} + upper-casing. Since a {@code $in} matches
 * stored values exactly, each raw ID is expanded to the stored spellings that used to normalize
 * to the same key (with/without a trailing ".0", as-is/upper-cased).
 */
public class ActiveInstrumentAttributeIdLookup {

    private final InstrumentAttributeRepository repository;

    private final Set<String> pendingInstrumentIds = new HashSet<>();
    private final Set<String> pendingAttributeIds  = new HashSet<>();

    // Keys resolved by the most recent flush (valid = found, checked = queried at all).
    private final Set<String> validInstrumentIds   = new HashSet<>();
    private final Set<String> validAttributeIds    = new HashSet<>();
    private final Set<String> checkedInstrumentIds = new HashSet<>();
    private final Set<String> checkedAttributeIds  = new HashSet<>();

    public ActiveInstrumentAttributeIdLookup(InstrumentAttributeRepository repository) {
        this.repository = repository;
    }

    public void reset() {
        pendingInstrumentIds.clear();
        pendingAttributeIds.clear();
        validInstrumentIds.clear();
        validAttributeIds.clear();
        checkedInstrumentIds.clear();
        checkedAttributeIds.clear();
    }

    /** Queues a row's IDs for the next flush; blank IDs are ignored (the validator reports them). */
    public void collect(String rawInstrumentId, String rawAttributeId) {
        if (!isBlank(rawInstrumentId)) pendingInstrumentIds.add(rawInstrumentId.trim());
        if (!isBlank(rawAttributeId))  pendingAttributeIds.add(rawAttributeId.trim());
    }

    /**
     * Makes {@link #validInstrumentIds()} / {@link #validAttributeIds()} authoritative for the
     * given row: flushes whatever {@link #collect} queued, then resolves the row's own IDs
     * individually if they were never collected (processor used without the read-listener wiring).
     */
    public void ensureChecked(String rawInstrumentId, String rawAttributeId) {
        if (!pendingInstrumentIds.isEmpty() || !pendingAttributeIds.isEmpty()) {
            flush(pendingInstrumentIds, pendingAttributeIds);
            pendingInstrumentIds.clear();
            pendingAttributeIds.clear();
        }

        Set<String> missingInstrumentIds = Collections.emptySet();
        Set<String> missingAttributeIds  = Collections.emptySet();
        if (!isBlank(rawInstrumentId) && !checkedInstrumentIds.contains(key(rawInstrumentId))) {
            missingInstrumentIds = Set.of(rawInstrumentId.trim());
        }
        if (!isBlank(rawAttributeId) && !checkedAttributeIds.contains(key(rawAttributeId))) {
            missingAttributeIds = Set.of(rawAttributeId.trim());
        }
        if (!missingInstrumentIds.isEmpty() || !missingAttributeIds.isEmpty()) {
            flush(missingInstrumentIds, missingAttributeIds);
        }
    }

    /** Normalized, upper-cased instrumentIds confirmed active by the latest flush. */
    public Set<String> validInstrumentIds() {
        return validInstrumentIds;
    }

    /** Normalized, upper-cased attributeIds confirmed active by the latest flush. */
    public Set<String> validAttributeIds() {
        return validAttributeIds;
    }

    private void flush(Set<String> instrumentIds, Set<String> attributeIds) {
        validInstrumentIds.clear();
        validAttributeIds.clear();
        checkedInstrumentIds.clear();
        checkedAttributeIds.clear();

        if (!instrumentIds.isEmpty()) {
            instrumentIds.forEach(id -> checkedInstrumentIds.add(key(id)));
            List<InstrumentAttribute> found = repository.findActiveInstrumentIdsIn(storedSpellings(instrumentIds));
            if (found != null) {
                for (InstrumentAttribute ia : found) {
                    if (ia.getInstrumentId() != null) validInstrumentIds.add(key(ia.getInstrumentId()));
                }
            }
        }
        if (!attributeIds.isEmpty()) {
            attributeIds.forEach(id -> checkedAttributeIds.add(key(id)));
            List<InstrumentAttribute> found = repository.findActiveAttributeIdsIn(storedSpellings(attributeIds));
            if (found != null) {
                for (InstrumentAttribute ia : found) {
                    if (ia.getAttributeId() != null) validAttributeIds.add(key(ia.getAttributeId()));
                }
            }
        }
    }

    private static Set<String> storedSpellings(Collection<String> rawIds) {
        Set<String> spellings = new HashSet<>();
        for (String raw : rawIds) {
            String trimmed = raw.trim();
            String normalized = TransactionActivityValidator.normalizeId(trimmed);
            for (String s : new String[]{trimmed, normalized, normalized + ".0"}) {
                spellings.add(s);
                spellings.add(s.toUpperCase());
            }
        }
        return spellings;
    }

    static String key(String rawId) {
        return TransactionActivityValidator.normalizeId(rawId.trim()).toUpperCase();
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
