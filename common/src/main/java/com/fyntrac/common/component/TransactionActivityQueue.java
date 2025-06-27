package com.fyntrac.common.component;

import com.fyntrac.common.cache.collection.CacheSet;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.repository.MemcachedRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.List;

@Component
public class TransactionActivityQueue {

    private static final String CACHE_KEY = "transaction:activity:%s:%d";
    private static final int TTL_SECONDS = 7200;

    private final MemcachedRepository memcachedRepository;

    @Autowired
    public TransactionActivityQueue(MemcachedRepository memcachedRepository) {
        this.memcachedRepository = memcachedRepository;
    }

    private String getKey(String tenantId, Long jobId) {
        return String.format(CACHE_KEY, tenantId, jobId);
    }

    public void add(String tenantId, Long jobId, TransactionActivity activity) {
        String key = getKey(tenantId, jobId);
        try {
            CacheSet<TransactionActivity> set = memcachedRepository.getFromCache(key, CacheSet.class);
            if (set == null) {
                set = new CacheSet<>();
            }
            set.add(activity);
            memcachedRepository.putInCache(key, set, TTL_SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Error adding record to Memcached", e);
        }
    }

    public int size(String tenantId, Long jobId) {
        try {
            CacheSet<TransactionActivity> set = memcachedRepository.getFromCache(getKey(tenantId, jobId), CacheSet.class);
            return set != null ? set.size() : 0;
        } catch (Exception e) {
            throw new RuntimeException("Error reading size from Memcached", e);
        }
    }

    public Iterator<TransactionActivity> getIterator(String tenantId, Long jobId) {
        try {
            CacheSet<TransactionActivity> set = memcachedRepository.getFromCache(getKey(tenantId, jobId), CacheSet.class);
            return set != null ? set.getSet().iterator() : null;
        } catch (Exception e) {
            throw new RuntimeException("Error reading size from Memcached", e);
        }
    }

    public void clear(String tenantId, Long jobId) {
        try {
            memcachedRepository.delete(getKey(tenantId, jobId));
        } catch (Exception e) {
            throw new RuntimeException("Error clearing Memcached key", e);
        }
    }

    /**
     * Reads one chunk of activities from Memcached using the built-in CacheSet pagination logic.
     *
     * @param tenantId  Tenant ID
     * @param jobId     Job ID
     * @param chunkSize Size of each chunk
     * @param chunkIndex Zero-based chunk index
     * @return List of activities in the chunk, or empty list if no such chunk
     */
    public List<TransactionActivity> readChunk(String tenantId, Long jobId, int chunkSize, int chunkIndex) {
        try {
            CacheSet<TransactionActivity> set = memcachedRepository.getFromCache(getKey(tenantId, jobId), CacheSet.class);
            if (set == null) {
                return List.of();
            }
            return set.getChunk(chunkSize, chunkIndex);
        } catch (Exception e) {
            throw new RuntimeException("Error reading chunk from Memcached", e);
        }
    }

    /**
     * Returns total number of chunks for given chunk size
     */
    public int getTotalChunks(String tenantId, Long jobId, int chunkSize) {
        try {
            CacheSet<TransactionActivity> set = memcachedRepository.getFromCache(getKey(tenantId, jobId), CacheSet.class);
            return set != null ? set.getTotalChunks(chunkSize) : 0;
        } catch (Exception e) {
            throw new RuntimeException("Error getting total chunks from Memcached", e);
        }
    }
}
