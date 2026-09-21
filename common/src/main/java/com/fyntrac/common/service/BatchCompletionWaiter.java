package com.fyntrac.common.service;

import com.fyntrac.common.repository.MemcachedRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
public class BatchCompletionWaiter {

    // Batch completions used to be tracked in an in-process ConcurrentHashMap<correlationId,
    // CompletableFuture>. That works only when there is exactly one instance of the dispatching
    // service: dataloader runs 2 replicas, and the Pulsar completion listener's callback can land
    // on either one — not necessarily the replica that dispatched the batch and is waiting here.
    // When it landed on the other replica, that replica's map had no entry for the correlationId,
    // the completion was silently dropped, and the waiting replica hung for the full timeout
    // (observed directly: a batch that completed in ~35s still took the full 10-minute timeout to
    // fail). Memcached is already shared across every replica, so writing/polling the result
    // there instead makes this correct regardless of which replica dispatches or completes a batch.
    private final MemcachedRepository memcachedRepository;

    private static final long POLL_INTERVAL_MS = 250L;
    // Comfortably longer than the largest timeoutMs any caller currently passes (10 minutes),
    // so a result never expires out of the cache while a caller could still be polling for it.
    private static final int CACHE_TTL_SECONDS = 900;

    public record BatchResult(String status, String payload, String errorMessage) implements Serializable {}

    @Autowired
    public BatchCompletionWaiter(MemcachedRepository memcachedRepository) {
        this.memcachedRepository = memcachedRepository;
    }

    private static String cacheKey(String correlationId) {
        return "batch-completion:" + correlationId;
    }

    /**
     * Polls Memcached for the batch's completion, parking the calling virtual thread between
     * checks (cheap: a virtual thread parked in Thread.sleep doesn't tie up a platform thread).
     * Unlike the old CompletableFuture-based wait, there is no need to register interest before
     * dispatching — completeBatch's write is durable in Memcached (with a TTL) whether it happens
     * before, during, or after this method starts polling, from any replica.
     * @param correlationId The unique ID for the batch, as passed to {@link #completeBatch(String, BatchResult)}.
     * @param timeoutMs Timeout in milliseconds.
     * @return The result of the batch processing.
     * @throws TimeoutException if the callback doesn't arrive within the timeout.
     */
    public BatchResult awaitCompletion(String correlationId, long timeoutMs) throws TimeoutException {
        log.info("Virtual thread suspending and waiting for correlationId: {}", correlationId);
        String key = cacheKey(correlationId);
        long deadline = System.currentTimeMillis() + timeoutMs;
        try {
            while (System.currentTimeMillis() < deadline) {
                BatchResult result = memcachedRepository.getFromCache(key, BatchResult.class);
                if (result != null) {
                    return result;
                }
                Thread.sleep(POLL_INTERVAL_MS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TimeoutException("Interrupted while waiting for batch completion: " + correlationId);
        }
        log.error("Error waiting for batch completion {}: timed out after {} ms", correlationId, timeoutMs);
        throw new TimeoutException("Batch processing timed out or failed: no completion received for " + correlationId);
    }

    /**
     * Called by the callback handler (Pulsar consumer), possibly on a different dataloader
     * replica than the one waiting — writes the result to Memcached so any replica's
     * {@link #awaitCompletion(String, long)} polling loop picks it up.
     */
    public void completeBatch(String correlationId, BatchResult result) {
        log.info("Completing batch for correlationId: {}.", correlationId);
        memcachedRepository.putInCache(cacheKey(correlationId), result, CACHE_TTL_SECONDS);
    }
}
