package com.reserv.dataloader.service.model.run;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.ExecutionInstance;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * One model execution per tenant at a time, across every dataloader pod.
 *
 * <p>Replaces an in-process lock that only held within one JVM: with more than one dataloader pod, two
 * requests landing on different pods ran the same tenant concurrently. The lock is a single document
 * in the tenant's database. A lock whose run has reached a final status is free, so a run that is
 * finished on another pod (a distributed run's finisher, an async request) releases it by finishing;
 * {@link #release} just tidies up. A lock older than {@code fyntrac.run.lock-max-age-hours} is also
 * free — the backstop for a run whose pods all died.
 */
@Slf4j
@Service
public class TenantExecutionLock {

    static final String LOCK_COLLECTION = "ExecutionLock";
    private static final String LOCK_ID = "model-execution";
    private static final Set<String> FINAL_STATUSES = Set.of("COMPLETED", "PARTIAL_SUCCESS", "FAILED");

    @Value("${fyntrac.run.lock-max-age-hours:24}")
    private long maxAgeHours;

    private final TenantDataSourceProvider dataSourceProvider;

    public TenantExecutionLock(TenantDataSourceProvider dataSourceProvider) {
        this.dataSourceProvider = dataSourceProvider;
    }

    /** @return the lock token if acquired, empty if another execution holds it */
    public Optional<String> tryAcquire(String tenant) {
        MongoTemplate mongo = dataSourceProvider.getDataSource(tenant);
        String token = UUID.randomUUID().toString();
        Document lock = new Document("_id", LOCK_ID)
                .append("token", token)
                .append("acquiredAt", new Date())
                .append("pod", System.getenv("HOSTNAME"));
        try {
            mongo.insert(lock, LOCK_COLLECTION);
            return Optional.of(token);
        } catch (DuplicateKeyException held) {
            Document current = mongo.findById(LOCK_ID, Document.class, LOCK_COLLECTION);
            if (current == null) {
                return tryAcquire(tenant);   // released between the insert and the read
            }
            if (!isFree(mongo, current)) {
                return Optional.empty();
            }
            // Take it over only if nobody else did first.
            Document replaced = mongo.findAndReplace(
                    new Query(Criteria.where("_id").is(LOCK_ID).and("token").is(current.getString("token"))),
                    lock, LOCK_COLLECTION);
            if (replaced == null) {
                return Optional.empty();
            }
            log.warn("Tenant {}: took over execution lock held by run {} (token {})", tenant,
                    current.getString("runId"), current.getString("token"));
            return Optional.of(token);
        }
    }

    /** Ties the lock to a run, so it frees itself when that run reaches a final status. */
    public void attachRun(String tenant, String token, String runId) {
        dataSourceProvider.getDataSource(tenant).updateFirst(
                new Query(Criteria.where("_id").is(LOCK_ID).and("token").is(token)),
                new Update().set("runId", runId), LOCK_COLLECTION);
    }

    public void release(String tenant, String token) {
        dataSourceProvider.getDataSource(tenant).remove(
                new Query(Criteria.where("_id").is(LOCK_ID).and("token").is(token)), LOCK_COLLECTION);
    }

    public void releaseForRun(String tenant, String runId) {
        dataSourceProvider.getDataSource(tenant).remove(
                new Query(Criteria.where("_id").is(LOCK_ID).and("runId").is(runId)), LOCK_COLLECTION);
    }

    /** Unconditional release, for an operator clearing a lock left by a run that will never finish. */
    public boolean forceRelease(String tenant) {
        return dataSourceProvider.getDataSource(tenant).remove(
                new Query(Criteria.where("_id").is(LOCK_ID)), LOCK_COLLECTION).getDeletedCount() > 0;
    }

    private boolean isFree(MongoTemplate mongo, Document lock) {
        Date acquiredAt = lock.getDate("acquiredAt");
        if (acquiredAt == null
                || System.currentTimeMillis() - acquiredAt.getTime() > TimeUnit.HOURS.toMillis(maxAgeHours)) {
            return true;
        }
        String runId = lock.getString("runId");
        if (runId == null) {
            return false;
        }
        // A missing run is not "finished": the lock is attached before the run record is written.
        ExecutionInstance run = mongo.findById(runId, ExecutionInstance.class);
        return run != null && FINAL_STATUSES.contains(run.getStatus());
    }
}
