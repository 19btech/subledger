package com.reserv.dataloader.service.model.run;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.ExecutionInstance;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Stamps {@code heartbeatAt} on the run records this pod is working on — pre-processing, planning, a
 * single-pod run's batches, a distributed run's finishing steps — so a run whose pod died can be told
 * apart from one that is merely slow ({@link DistributedRunService#reapIfAbandoned}). A distributed
 * run's chunks have their own heartbeat in ExecutionRunChunk.
 */
@Slf4j
@Component
public class RunHeartbeat {

    @Value("${fyntrac.run.heartbeat-seconds:30}")
    private long heartbeatSeconds;

    private final TenantDataSourceProvider dataSourceProvider;
    private final String podName;
    // runId -> tenant
    private final Map<String, String> runs = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "run-heartbeat");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean started;

    public RunHeartbeat(TenantDataSourceProvider dataSourceProvider) {
        this.dataSourceProvider = dataSourceProvider;
        String host = System.getenv("HOSTNAME");
        this.podName = host != null ? host : "pod-" + ProcessHandle.current().pid();
    }

    /** Starts heartbeating the run (and stamps it now). */
    public void register(String tenant, String runId) {
        runs.put(runId, tenant);
        beat(runId, tenant);
        start();
    }

    public void unregister(String runId) {
        runs.remove(runId);
    }

    private synchronized void start() {
        if (started) return;
        started = true;
        scheduler.scheduleWithFixedDelay(() -> runs.forEach(this::beat), heartbeatSeconds, heartbeatSeconds, TimeUnit.SECONDS);
    }

    private void beat(String runId, String tenant) {
        try {
            dataSourceProvider.getDataSource(tenant).updateFirst(
                    new Query(Criteria.where("_id").is(runId)),
                    new Update().set("heartbeatAt", new Date()).set("heartbeatPod", podName),
                    ExecutionInstance.class);
        } catch (Exception e) {
            log.warn("Run heartbeat failed for run {} (tenant {}): {}", runId, tenant, e.getMessage());
        }
    }

    @PreDestroy
    public void stop() {
        scheduler.shutdownNow();
    }
}
