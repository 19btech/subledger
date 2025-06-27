package com.fyntrac.common.component;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.repository.MemcachedRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class InstrumentReplaySet {

    private static final String CACHE_KEY = "replay:instrument:%s:%d";
    private static final int TTL_SECONDS = 7200;

    private final MemcachedRepository memcachedRepository;

    @Autowired
    public InstrumentReplaySet(MemcachedRepository memcachedRepository) {
        this.memcachedRepository = memcachedRepository;
    }

    public void add(String tenantId, Long jobId, Records.InstrumentReplayRecord newRecord) {
        String key = String.format(CACHE_KEY, tenantId, jobId);
        try {
            Set<Records.InstrumentReplayRecord> recordSet = memcachedRepository.getFromCache(key, Set.class);
            if (recordSet == null) {
                recordSet = new LinkedHashSet<>();
            }

            // Remove duplicate if same instrumentId but older effectiveDate
            recordSet.removeIf(existing ->
                    existing.instrumentId().equals(newRecord.instrumentId()) &&
                            existing.effectiveDate() > newRecord.effectiveDate());

            recordSet.add(newRecord);
            memcachedRepository.putInCache(key, recordSet, TTL_SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Error adding record to Memcached", e);
        }
    }

    /**
     * Read a chunk of replay records by index without removing them.
     */
    public List<Records.InstrumentReplayRecord> readChunk(String tenantId, Long jobId, int chunkSize, int chunkIndex) {
        String key = String.format(CACHE_KEY, tenantId, jobId);
        try {
            Set<Records.InstrumentReplayRecord> recordSet = memcachedRepository.getFromCache(key, Set.class);
            if (recordSet == null || recordSet.isEmpty()) return Collections.emptyList();

            List<Records.InstrumentReplayRecord> allRecords = new ArrayList<>(recordSet);
            int fromIndex = chunkIndex * chunkSize;
            int toIndex = Math.min(fromIndex + chunkSize, allRecords.size());

            if (fromIndex >= allRecords.size()) return Collections.emptyList();

            return allRecords.subList(fromIndex, toIndex);
        } catch (Exception e) {
            throw new RuntimeException("Error reading chunk from Memcached", e);
        }
    }

    public void clear(String tenantId, Long jobId) {
        try {
            memcachedRepository.delete(String.format(CACHE_KEY, tenantId, jobId));
        } catch (Exception e) {
            throw new RuntimeException("Error clearing Memcached key", e);
        }
    }

    public int size(String tenantId, Long jobId) {
        try {
            Set<Records.InstrumentReplayRecord> recordSet = memcachedRepository.getFromCache(String.format(CACHE_KEY, tenantId, jobId), Set.class);
            return recordSet != null ? recordSet.size() : 0;
        } catch (Exception e) {
            throw new RuntimeException("Error reading size from Memcached", e);
        }
    }
}
