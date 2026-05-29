package com.fyntrac.common.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
public class BatchCompletionWaiter {

    // Maps correlationId -> CompletableFuture that will be completed when the callback arrives
    private final Map<String, CompletableFuture<BatchResult>> pendingBatches = new ConcurrentHashMap<>();

    public record BatchResult(String status, String payload, String errorMessage) {}

    /**
     * Suspends the calling virtual thread until the batch completion callback is received.
     * @param correlationId The unique ID for the batch.
     * @param timeoutMs Timeout in milliseconds.
     * @return The result of the batch processing.
     * @throws TimeoutException if the callback doesn't arrive within the timeout.
     */
    public BatchResult waitForCompletion(String correlationId, long timeoutMs) throws TimeoutException {
        CompletableFuture<BatchResult> future = new CompletableFuture<>();
        pendingBatches.put(correlationId, future);

        try {
            log.info("Virtual thread suspending and waiting for correlationId: {}", correlationId);
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("Error waiting for batch completion {}: {}", correlationId, e.getMessage());
            throw new TimeoutException("Batch processing timed out or failed: " + e.getMessage());
        } finally {
            pendingBatches.remove(correlationId);
        }
    }

    /**
     * Called by the callback handler (Pulsar consumer) to wake up the suspended virtual thread.
     */
    public void completeBatch(String correlationId, BatchResult result) {
        CompletableFuture<BatchResult> future = pendingBatches.get(correlationId);
        if (future != null) {
            log.info("Completing batch for correlationId: {}. Waking up virtual thread.", correlationId);
            future.complete(result);
        } else {
            log.warn("Received completion for correlationId {} but no virtual thread is waiting.", correlationId);
        }
    }
}
