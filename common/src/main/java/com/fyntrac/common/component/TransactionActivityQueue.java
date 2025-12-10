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
    private static final String LOCK_KEY = "lock:transaction:activity:%s:%d"; // New Lock Key
    private static final int TTL_SECONDS = 7200;
    private static final int LOCK_TIMEOUT_SECONDS = 5; // Safety release if app crashes
    private static final int MAX_RETRIES = 20;
    private static final long RETRY_DELAY_MS = 100;

    private final MemcachedRepository memcachedRepository;

    @Autowired
    public TransactionActivityQueue(MemcachedRepository memcachedRepository) {
        this.memcachedRepository = memcachedRepository;
    }

    private String getKey(String tenantId, Long jobId) {
        return String.format(CACHE_KEY, tenantId, jobId);
    }

    private String getLockKey(String tenantId, Long jobId) {
        return String.format(LOCK_KEY, tenantId, jobId);
    }

    /**
     * Adds an activity to the queue with Distributed Locking to prevent race conditions.
     */
    public void add(String tenantId, Long jobId, TransactionActivity activity) {
        String key = getKey(tenantId, jobId);
        String lockKey = getLockKey(tenantId, jobId);

        boolean lockAcquired = false;
        int attempts = 0;

        try {
            // 1. Try to acquire lock (Spin Lock mechanism)
            while (attempts < MAX_RETRIES) {
                // 'add' is atomic in Memcached. It returns true ONLY if the key didn't exist.
                // We set a short TTL (5s) so if the server crashes, the lock auto-releases.
                lockAcquired = memcachedRepository.add(lockKey, "LOCKED", LOCK_TIMEOUT_SECONDS);

                if (lockAcquired) {
                    break;
                }

                // Wait and retry
                Thread.sleep(RETRY_DELAY_MS);
                attempts++;
            }

            if (!lockAcquired) {
                throw new RuntimeException("Could not acquire lock to add transaction activity after " + MAX_RETRIES + " attempts.");
            }

            // 2. Critical Section: Read - Modify - Write
            CacheSet<TransactionActivity> set = memcachedRepository.getFromCache(key, CacheSet.class);
            if (set == null) {
                set = new CacheSet<>();
            }
            set.add(activity);
            memcachedRepository.putInCache(key, set, TTL_SECONDS);

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted while waiting for lock", ie);
        } catch (Exception e) {
            throw new RuntimeException("Error adding record to Memcached", e);
        } finally {
            // 3. Always release the lock
            if (lockAcquired) {
                try {
                    memcachedRepository.delete(lockKey);
                } catch (Exception e) {
                    // Log error but don't fail, TTL will clean it up eventually
                    System.err.println("Failed to release lock: " + e.getMessage());
                }
            }
        }
    }

    // ... (rest of your methods: size, getIterator, clear, readChunk, etc. remain the same)
    // Note: readChunk usually doesn't need locking unless you need strict consistency while writing.

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

    public int getTotalChunks(String tenantId, Long jobId, int chunkSize) {
        try {
            CacheSet<TransactionActivity> set = memcachedRepository.getFromCache(getKey(tenantId, jobId), CacheSet.class);
            return set != null ? set.getTotalChunks(chunkSize) : 0;
        } catch (Exception e) {
            throw new RuntimeException("Error getting total chunks from Memcached", e);
        }
    }
}