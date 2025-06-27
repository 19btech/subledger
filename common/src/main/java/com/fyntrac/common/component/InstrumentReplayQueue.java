package com.fyntrac.common.component;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.repository.MemcachedRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class InstrumentReplayQueue {

    private static final String CACHE_KEY = "replay:instrument:queue:%s:%d"; // tenantId
    private static final int TTL_SECONDS = 7200; // 1 hour

    private final MemcachedRepository memcachedRepository;

    @Autowired
    public InstrumentReplayQueue(MemcachedRepository memcachedRepository) {
        this.memcachedRepository = memcachedRepository;
    }
    public void add(String tenantId, Long jobId, Records.InstrumentReplayRecord newRecord) {
        String key = String.format(CACHE_KEY, tenantId,jobId);
        try {
            Map<String, Records.InstrumentReplayRecord> recordMap = memcachedRepository.getFromCache(key, Map.class);
            if (recordMap == null) {
                recordMap = new LinkedHashMap<>();
            }

            Records.InstrumentReplayRecord  existing = recordMap.get(newRecord.instrumentId());
            if (existing == null || newRecord.effectiveDate() < existing.effectiveDate()) {
                recordMap.put(newRecord.instrumentId(), newRecord);
                memcachedRepository.putInCache(key, recordMap, TTL_SECONDS);
            }

        } catch (Exception e) {
            throw new RuntimeException("Error adding record to Memcached", e);
        }
    }

    public Records.InstrumentReplayRecord poll(String tenantId, Long jobId) {
        String key = String.format(CACHE_KEY, tenantId, jobId);
        try {
            Map<String, Records.InstrumentReplayRecord> recordMap = memcachedRepository.getFromCache(key, Map.class);
            if (recordMap == null || recordMap.isEmpty()) return null;

            Iterator<Map.Entry<String, Records.InstrumentReplayRecord>> iterator = recordMap.entrySet().iterator();
            if (iterator.hasNext()) {
                Map.Entry<String, Records.InstrumentReplayRecord> entry = iterator.next();
                iterator.remove(); // Remove from map
                memcachedRepository.putInCache(key, recordMap, TTL_SECONDS); // Update Memcached
                return entry.getValue();
            }

            return null;
        } catch (Exception e) {
            throw new RuntimeException("Error polling record from Memcached", e);
        }
    }

    public void clear(String tenantId, Long jobId) {
        try {
            memcachedRepository.delete(String.format(CACHE_KEY, tenantId,jobId));
        } catch (Exception e) {
            throw new RuntimeException("Error clearing Memcached key", e);
        }
    }

    public int size(String tenantId, Long jobId) {
        try {
            Map<String, Records.InstrumentReplayRecord> recordMap = memcachedRepository.getFromCache(String.format(CACHE_KEY, tenantId, jobId), Map.class);
            return recordMap != null ? recordMap.size() : 0;
        } catch (Exception e) {
            throw new RuntimeException("Error reading size from Memcached", e);
        }
    }
}