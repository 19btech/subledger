package com.fyntrac.common.service;

import com.fyntrac.common.config.TenantContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.TimeUnit;
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
    // fail).
    //
    // They then moved to Memcached, which fixed that but is not a place for state that exists
    // nowhere else: every TransactionActivity upload, for any tenant, flushes all of Memcached
    // (TransactionActivityJobCompletionListener), and entries are evicted under memory pressure. A
    // completion lost that way made the waiting batch time out and fail although the model had
    // finished it. Completions are now documents in one shared MongoDB database — shared because
    // the completion listener has no tenant context and the waiter does.
    private static final String COLLECTION = "BatchCompletion";
    private static final long POLL_INTERVAL_MS = 250L;
    // Kept long past the largest timeoutMs any caller passes (10 minutes); MongoDB expires them.
    private static final long RETENTION_SECONDS = TimeUnit.DAYS.toSeconds(1);

    public record BatchResult(String status, String payload, String errorMessage) implements Serializable {}

    // Resolved lazily: common is scanned by services that never wait on a batch.
    private final ObjectProvider<MongoTemplate> mongoTemplateProvider;

    @Value("${fyntrac.batch-completion.database:master}")
    private String database;

    private volatile boolean indexEnsured;

    @Autowired
    public BatchCompletionWaiter(ObjectProvider<MongoTemplate> mongoTemplateProvider) {
        this.mongoTemplateProvider = mongoTemplateProvider;
    }

    /**
     * Polls for the batch's completion, parking the calling virtual thread between checks (cheap: a
     * virtual thread parked in Thread.sleep doesn't tie up a platform thread). There is no need to
     * register interest before dispatching — completeBatch's write is durable whether it happens
     * before, during, or after this method starts polling, from any replica.
     * @param correlationId The unique ID for the batch, as passed to {@link #completeBatch(String, BatchResult)}.
     * @param timeoutMs Timeout in milliseconds.
     * @return The result of the batch processing.
     * @throws TimeoutException if the callback doesn't arrive within the timeout.
     */
    public BatchResult awaitCompletion(String correlationId, long timeoutMs) throws TimeoutException {
        log.info("Virtual thread suspending and waiting for correlationId: {}", correlationId);
        long deadline = System.currentTimeMillis() + timeoutMs;
        try {
            while (System.currentTimeMillis() < deadline) {
                Document done = inSharedDatabase(mongo -> mongo.findById(correlationId, Document.class, COLLECTION));
                if (done != null) {
                    return new BatchResult(done.getString("status"), done.getString("payload"), done.getString("errorMessage"));
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
     * replica than the one waiting — records the result so any replica's
     * {@link #awaitCompletion(String, long)} polling loop picks it up.
     */
    public void completeBatch(String correlationId, BatchResult result) {
        log.info("Completing batch for correlationId: {}.", correlationId);
        Document done = new Document("_id", correlationId)
                .append("status", result.status())
                .append("payload", result.payload())
                .append("errorMessage", result.errorMessage())
                .append("completedAt", new Date());
        inSharedDatabase(mongo -> {
            ensureIndex(mongo);
            mongo.save(done, COLLECTION);
            return null;
        });
    }

    // The template is tenant-aware (database = current tenant), so pin the shared database for the
    // call; runWithTenant restores the caller's tenant afterwards.
    private <T> T inSharedDatabase(java.util.function.Function<MongoTemplate, T> call) {
        MongoTemplate mongo = mongoTemplateProvider.getObject();
        return TenantContextHolder.runWithTenant(database, () -> call.apply(mongo));
    }

    private void ensureIndex(MongoTemplate mongo) {
        if (indexEnsured) return;
        mongo.indexOps(COLLECTION).ensureIndex(new Index().on("completedAt", Sort.Direction.ASC)
                .expire(RETENTION_SECONDS, TimeUnit.SECONDS));
        indexEnsured = true;
    }
}
