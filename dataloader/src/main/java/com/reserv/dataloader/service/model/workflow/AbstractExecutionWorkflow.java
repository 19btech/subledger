package com.reserv.dataloader.service.model.workflow;

import com.fyntrac.common.entity.ExecutionInstance;
import com.fyntrac.common.repository.ExecutionInstanceRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.UUID;

@Slf4j
public abstract class AbstractExecutionWorkflow {

    protected final ExecutionInstanceRepository executionInstanceRepository;

    // Tenant-aware (MultiTenantMongoDbFactory). Status and counters are written as targeted
    // $set / $inc updates rather than save(instance): save() writes back this pod's whole in-memory
    // copy, so two pods working one run (TARGET_DESIGN_DISTRIBUTED_RUN.md) would overwrite each
    // other's counts. Today one pod owns a run and the results are the same either way.
    @Autowired
    private MongoTemplate mongoTemplate;

    public AbstractExecutionWorkflow(ExecutionInstanceRepository executionInstanceRepository) {
        this.executionInstanceRepository = executionInstanceRepository;
    }

    /**
     * The Template Method governing the execution flow.
     */
    public final void executeWorkflow(String tenant, int postingDate) throws Throwable {
        startWorkflow(tenant, postingDate, UUID.randomUUID().toString());
    }

    /**
     * Runs the workflow under the given run id. When {@link #tailRunsElsewhere()} is true, this
     * returns once the run's work has been handed out: post-processing, EOD and the final status
     * are then done by whichever pod completes the run's last chunk, through {@link #completeRun}
     * (TARGET_DESIGN_DISTRIBUTED_RUN.md). Otherwise the whole run happens here, as before.
     */
    public final ExecutionInstance startWorkflow(String tenant, int postingDate, String runId) throws Throwable {
        ExecutionInstance instance = initializeInstance(runId, tenant, postingDate);
        log.info("Starting execution workflow: instanceId={} tenant={} postingDate={}", instance.getId(), tenant, postingDate);

        try {
            // 1. Pre-Processing
            updateStatus(instance, "PRE_PROCESSING");
            boolean shouldContinue = preProcess(tenant, postingDate);
            if (!shouldContinue) {
                instance.setEndTime(new Date());
                updateStatus(instance, "COMPLETED");
                log.info("Workflow execution halted gracefully by pre-process rules.");
                return instance;
            }

            // 2. Event Generation and Processing
            updateStatus(instance, "GENERATING_EVENTS");
            generateAndProcessEvents(instance, tenant, postingDate);
            if (tailRunsElsewhere()) {
                return instance;
            }

            runTail(instance, tenant, postingDate);
            return instance;

        } catch (Exception e) {
            handleFailure(instance, e);
            throw e;
        }
    }

    /**
     * Finishes a run whose batches were processed on other pods: post-processing, EOD and the final
     * status. Called by exactly one pod — the one that completed the run's last chunk. Never throws;
     * a failure marks the run FAILED.
     */
    public final void completeRun(ExecutionInstance instance) {
        try {
            runTail(instance, instance.getTenantId(), instance.getPostingDate());
        } catch (Exception e) {
            handleFailure(instance, e);
        }
    }

    private void runTail(ExecutionInstance instance, String tenant, int postingDate) {
        // 3. Post Processing
        updateStatus(instance, "POST_PROCESSING");
        postProcess(instance, tenant, postingDate);

        // 4. End Of Day
        updateStatus(instance, "EOD_PROCESSING");
        performEOD(instance);

        // Complete — a batch that hit its own catch block (see DslExecutionWorkflow) doesn't
        // throw here anymore, so this "success" path is also where a degraded run surfaces.
        instance.setEndTime(new Date());
        refreshCounters(instance);
        boolean hadFailedBatches = instance.getFailedBatches() != null && instance.getFailedBatches() > 0;
        updateStatus(instance, hadFailedBatches ? "PARTIAL_SUCCESS" : "COMPLETED");
    }

    /** True when {@link #generateAndProcessEvents} hands the run to other pods instead of finishing it. */
    protected boolean tailRunsElsewhere() {
        return false;
    }

    private ExecutionInstance initializeInstance(String runId, String tenant, int postingDate) {
        ExecutionInstance instance = ExecutionInstance.builder()
                .id(runId)
                .tenantId(tenant)
                .postingDate(postingDate)
                .modelType(getModelType())
                .status("INITIALIZING")
                .startTime(new Date())
                .build();
        return executionInstanceRepository.save(instance);
    }

    protected void updateStatus(ExecutionInstance instance, String status) {
        instance.setStatus(status);
        Update update = new Update().set("status", status);
        if (instance.getEndTime() != null) {
            update.set("endTime", instance.getEndTime());
        }
        mongoTemplate.updateFirst(byId(instance), update, ExecutionInstance.class);
    }

    protected void handleFailure(ExecutionInstance instance, Exception e) {
        log.error("Orchestration failed for instance {}: {}", instance.getId(), e.getMessage());
        instance.setStatus("FAILED");
        instance.setErrorMessage(e.getMessage());
        mongoTemplate.updateFirst(byId(instance),
                new Update().set("status", "FAILED").set("errorMessage", e.getMessage()), ExecutionInstance.class);
    }

    protected void incrementCompletedBatches(ExecutionInstance instance) {
        instance.setCompletedBatches(increment(instance, "completedBatches").getCompletedBatches());
    }

    protected void incrementFailedBatches(ExecutionInstance instance) {
        instance.setFailedBatches(increment(instance, "failedBatches").getFailedBatches());
    }

    // Atomic $inc, returning the stored document so the local copy reflects every writer's increments.
    private ExecutionInstance increment(ExecutionInstance instance, String field) {
        return mongoTemplate.findAndModify(byId(instance), new Update().inc(field, 1),
                FindAndModifyOptions.options().returnNew(true), ExecutionInstance.class);
    }

    // The final status must count every writer's failures, not just this pod's.
    private void refreshCounters(ExecutionInstance instance) {
        ExecutionInstance stored = mongoTemplate.findOne(byId(instance), ExecutionInstance.class);
        if (stored != null) {
            instance.setCompletedBatches(stored.getCompletedBatches());
            instance.setFailedBatches(stored.getFailedBatches());
        }
    }

    private static Query byId(ExecutionInstance instance) {
        return new Query(Criteria.where("_id").is(instance.getId()));
    }

    protected abstract String getModelType();
    protected abstract boolean preProcess(String tenant, int postingDate) throws Throwable;
    protected abstract void generateAndProcessEvents(ExecutionInstance instance, String tenant, int postingDate) throws Throwable;
    protected abstract void postProcess(ExecutionInstance instance, String tenant, int postingDate);
    protected abstract void performEOD(ExecutionInstance instance);
}
