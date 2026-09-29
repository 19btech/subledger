package com.reserv.dataloader.service.model.run;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.ExecutionInstance;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.result.UpdateResult;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Bookkeeping for a DSL run spread across dataloader pods (TARGET_DESIGN_DISTRIBUTED_RUN.md).
 *
 * <p>The coordinator {@link #plan}s the run into chunks of instruments, records them in
 * {@code ExecutionRunChunk} and the chunk count on the run's {@code ExecutionInstance}, then publishes
 * them ({@link RunChunkQueue}). A pod that takes a chunk {@link #claim}s it, heartbeats it while it
 * works, and {@link #finishChunk finishes} it — which atomically counts it on the run. The pod whose
 * count closes out the run, and wins the {@code finalized} compare-and-set, runs the run's tail.
 *
 * <p>Chunk states: PLANNED → IN_PROGRESS → DONE | FAILED. Every transition is a compare-and-set, so a
 * redelivered message can never count a chunk twice.
 */
@Slf4j
@Service
public class DistributedRunService {

    static final String CHUNK_COLLECTION = "ExecutionRunChunk";
    private static final Set<String> FINAL_STATUSES = Set.of("COMPLETED", "PARTIAL_SUCCESS", "FAILED");

    public enum Claim {
        /** This pod now owns the chunk and must process and finish it. */
        CLAIMED,
        /** The chunk is DONE or FAILED already — a duplicate delivery; nothing to do. */
        ALREADY_FINISHED,
        /** Another pod owns the chunk and is still heartbeating it; offer it again later. */
        OWNED_ELSEWHERE,
        /** The owning pod stopped heartbeating: it died mid-chunk. The chunk must be failed. */
        OWNER_LOST
    }

    @Value("${fyntrac.run.distributed:false}")
    private boolean enabled;

    // Instruments per chunk. Each chunk is paged and split into model batches exactly as a whole run
    // used to be, so this only sets how finely a run spreads across pods: ~1,600 chunks at 8M.
    @Value("${fyntrac.run.chunk-size:5000}")
    private int chunkSize;

    @Value("${fyntrac.run.chunk-heartbeat-seconds:30}")
    private long heartbeatSeconds;

    // A chunk whose heartbeat is older than this is treated as abandoned by a dead pod. Must be well
    // above the heartbeat interval: a pod that is merely slow must not lose its chunk.
    @Value("${fyntrac.run.chunk-stale-seconds:300}")
    private long staleSeconds;

    @Value("${fyntrac.run.status-poll-seconds:2}")
    private long statusPollSeconds;

    private final TenantDataSourceProvider dataSourceProvider;
    private final RunChunkQueue runChunkQueue;
    private final String podName;
    private final Set<String> indexedTenants = ConcurrentHashMap.newKeySet();
    // tenant -> chunk ids this pod is working on, heartbeated together.
    private final ConcurrentHashMap<String, Set<String>> ownedChunks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "run-chunk-heartbeat");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean heartbeatStarted;

    public DistributedRunService(TenantDataSourceProvider dataSourceProvider, RunChunkQueue runChunkQueue) {
        this.dataSourceProvider = dataSourceProvider;
        this.runChunkQueue = runChunkQueue;
        String host = System.getenv("HOSTNAME");
        this.podName = host != null ? host : "pod-" + ProcessHandle.current().pid();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String podName() {
        return podName;
    }

    // ── Coordinator ──────────────────────────────────────────────────────────

    /**
     * Cuts the tenant's active instruments into chunks of {@code chunkSize} and records them, and the
     * chunk count on the run, before anything is published — a chunk must never complete before the
     * run knows how many there are.
     */
    public List<RunChunkMessage> plan(String tenant, String runId, int postingDate, boolean sharedReferenceEventsExist) {
        MongoTemplate mongo = mongo(tenant);
        long startNanos = System.nanoTime();

        // One ordered pass over the active instrument ids, the same filter and order event generation
        // pages through, so each chunk is exactly the instruments generateEventAndDispatch will see
        // for its range. Instruments have one active row per sub-instrument; count distinct ids.
        List<RunChunkMessage> chunks = new ArrayList<>();
        long instruments = 0;
        String previous = null;
        String lowerBound = null;
        int inChunk = 0;
        try (MongoCursor<Document> cursor = mongo.getCollection(mongo.getCollectionName(InstrumentAttribute.class))
                .find(new Document("endDate", null))
                .projection(new Document("instrumentId", 1).append("_id", 0))
                .sort(new Document("instrumentId", 1))
                .batchSize(10_000)
                .iterator()) {
            while (cursor.hasNext()) {
                Object value = cursor.next().get("instrumentId");
                if (value == null) continue;
                String id = value.toString();
                if (id.equals(previous)) continue;
                previous = id;
                instruments++;
                if (++inChunk == chunkSize) {
                    chunks.add(new RunChunkMessage(tenant, runId, postingDate, chunks.size(), lowerBound, id,
                            sharedReferenceEventsExist));
                    lowerBound = id;
                    inChunk = 0;
                }
            }
        }
        if (inChunk > 0) {
            // Open-ended, like the single-pod walk: it runs to the end of the collection.
            chunks.add(new RunChunkMessage(tenant, runId, postingDate, chunks.size(), lowerBound, null,
                    sharedReferenceEventsExist));
        }

        if (indexedTenants.add(tenant)) {
            mongo.indexOps(CHUNK_COLLECTION).ensureIndex(new Index().on("runId", Sort.Direction.ASC));
        }
        if (!chunks.isEmpty()) {
            Date now = new Date();
            List<Document> docs = new ArrayList<>(chunks.size());
            for (RunChunkMessage chunk : chunks) {
                docs.add(new Document("_id", chunk.chunkId())
                        .append("runId", runId)
                        .append("postingDate", postingDate)
                        .append("seq", chunk.seq())
                        .append("fromExclusive", chunk.fromExclusive())
                        .append("toInclusive", chunk.toInclusive())
                        .append("status", "PLANNED")
                        .append("attempts", 0)
                        .append("createdAt", now));
            }
            mongo.getCollection(CHUNK_COLLECTION).insertMany(docs);
        }
        mongo.updateFirst(runQuery(runId), new Update()
                        .set("totalChunks", chunks.size())
                        .set("completedChunks", 0)
                        .set("failedChunks", 0)
                        .set("totalInstruments", instruments)
                        .set("finalized", false),
                ExecutionInstance.class);

        log.info("Planned run {} (tenant {}, postingDate {}): {} instruments in {} chunks of up to {} ({} ms)",
                runId, tenant, postingDate, instruments, chunks.size(), chunkSize,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        return chunks;
    }

    public void publish(List<RunChunkMessage> chunks) throws Exception {
        runChunkQueue.publishAll(chunks);
    }

    /** The run's ExecutionInstance record as stored, including the chunk counters the entity lacks. */
    public Document runStatus(String tenant, String runId) {
        MongoTemplate mongo = mongo(tenant);
        return mongo.findById(runId, Document.class, mongo.getCollectionName(ExecutionInstance.class));
    }

    /** Blocks until the run reaches a final status, and returns it. */
    public String awaitFinalStatus(String tenant, String runId) throws InterruptedException {
        while (true) {
            Document run = runStatus(tenant, runId);
            String status = run == null ? null : run.getString("status");
            if (status == null || FINAL_STATUSES.contains(status)) {
                return status;
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(statusPollSeconds));
        }
    }

    // ── Chunk owner ──────────────────────────────────────────────────────────

    public Claim claim(RunChunkMessage chunk) {
        MongoTemplate mongo = mongo(chunk.tenantId());
        Date now = new Date();
        UpdateResult claimed = mongo.updateFirst(
                new Query(Criteria.where("_id").is(chunk.chunkId()).and("status").is("PLANNED")),
                new Update().set("status", "IN_PROGRESS")
                        .set("owner", podName)
                        .set("startedAt", now)
                        .set("heartbeatAt", now)
                        .inc("attempts", 1),
                CHUNK_COLLECTION);
        if (claimed.getModifiedCount() == 1) {
            ownedChunks.computeIfAbsent(chunk.tenantId(), t -> ConcurrentHashMap.newKeySet()).add(chunk.chunkId());
            startHeartbeat();
            return Claim.CLAIMED;
        }

        Document current = mongo.findById(chunk.chunkId(), Document.class, CHUNK_COLLECTION);
        if (current == null) {
            // Not planned in this tenant — a message for a run that no longer exists. Drop it.
            log.warn("Run chunk {} has no ExecutionRunChunk record; dropping the message", chunk.chunkId());
            return Claim.ALREADY_FINISHED;
        }
        String status = current.getString("status");
        if (!"IN_PROGRESS".equals(status)) {
            return Claim.ALREADY_FINISHED;
        }
        Date heartbeatAt = current.getDate("heartbeatAt");
        boolean stale = heartbeatAt == null
                || now.getTime() - heartbeatAt.getTime() > TimeUnit.SECONDS.toMillis(staleSeconds);
        return stale ? Claim.OWNER_LOST : Claim.OWNED_ELSEWHERE;
    }

    /**
     * Marks the chunk DONE or FAILED and counts it on the run. {@code asOwner} = this pod claimed it;
     * otherwise this is a takeover of a chunk whose owner was lost ({@link Claim#OWNER_LOST}).
     *
     * @return true if this call closed out the run and won its finalization — the caller must then
     *         run the tail ({@code AbstractExecutionWorkflow.completeRun}) exactly once
     */
    public boolean finishChunk(RunChunkMessage chunk, boolean asOwner, boolean failed, String error) {
        MongoTemplate mongo = mongo(chunk.tenantId());
        Set<String> owned = ownedChunks.get(chunk.tenantId());
        if (owned != null) owned.remove(chunk.chunkId());

        Criteria transition = Criteria.where("_id").is(chunk.chunkId()).and("status").is("IN_PROGRESS");
        if (asOwner) transition = transition.and("owner").is(podName);
        Update update = new Update().set("status", failed ? "FAILED" : "DONE").set("endedAt", new Date());
        if (error != null) update.set("error", error);
        if (!asOwner) update.set("failedBy", podName);
        if (mongo.updateFirst(new Query(transition), update, CHUNK_COLLECTION).getModifiedCount() == 0) {
            // Someone else already finished it (a takeover, or a duplicate) — it is counted once, there.
            log.warn("Run chunk {} was already finished elsewhere; not counting it again", chunk.chunkId());
            return false;
        }

        // A failed chunk also counts as a failed batch, in the same atomic update, so the run's final
        // status (PARTIAL_SUCCESS when failedBatches > 0) sees it before any tail can start.
        Update count = new Update().inc("completedChunks", 1);
        if (failed) count.inc("failedChunks", 1).inc("failedBatches", 1);
        Document run = mongo.findAndModify(runQuery(chunk.runId()), count,
                FindAndModifyOptions.options().returnNew(true), Document.class,
                mongo.getCollectionName(ExecutionInstance.class));
        if (run == null) return false;
        int completed = run.getInteger("completedChunks", 0);
        int total = run.getInteger("totalChunks", Integer.MAX_VALUE);
        log.info("Run {} chunk {} {} ({}/{} chunks)", chunk.runId(), chunk.seq(), failed ? "FAILED" : "done",
                completed, total);
        return completed >= total && tryFinalize(chunk.tenantId(), chunk.runId());
    }

    /** Compare-and-set of the run's finalized flag: true for exactly one caller per run. */
    public boolean tryFinalize(String tenant, String runId) {
        return mongo(tenant).updateFirst(
                new Query(Criteria.where("_id").is(runId).and("finalized").ne(true)),
                new Update().set("finalized", true).set("finalizedBy", podName),
                ExecutionInstance.class).getModifiedCount() == 1;
    }

    private synchronized void startHeartbeat() {
        if (heartbeatStarted) return;
        heartbeatStarted = true;
        heartbeat.scheduleWithFixedDelay(this::beat, heartbeatSeconds, heartbeatSeconds, TimeUnit.SECONDS);
    }

    private void beat() {
        ownedChunks.forEach((tenant, ids) -> {
            if (ids.isEmpty()) return;
            try {
                mongo(tenant).updateMulti(
                        new Query(Criteria.where("_id").in(ids).and("status").is("IN_PROGRESS").and("owner").is(podName)),
                        new Update().set("heartbeatAt", new Date()),
                        CHUNK_COLLECTION);
            } catch (Exception e) {
                log.warn("Run-chunk heartbeat failed for tenant {}: {}", tenant, e.getMessage());
            }
        });
    }

    @PreDestroy
    public void stop() {
        heartbeat.shutdownNow();
    }

    private MongoTemplate mongo(String tenant) {
        return dataSourceProvider.getDataSource(tenant);
    }

    private static Query runQuery(String runId) {
        return new Query(Criteria.where("_id").is(runId));
    }
}
