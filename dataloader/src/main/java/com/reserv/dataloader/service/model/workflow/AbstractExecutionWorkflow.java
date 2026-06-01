package com.reserv.dataloader.service.model.workflow;

import com.fyntrac.common.entity.ExecutionInstance;
import com.fyntrac.common.repository.ExecutionInstanceRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.UUID;

@Slf4j
public abstract class AbstractExecutionWorkflow {

    protected final ExecutionInstanceRepository executionInstanceRepository;

    public AbstractExecutionWorkflow(ExecutionInstanceRepository executionInstanceRepository) {
        this.executionInstanceRepository = executionInstanceRepository;
    }

    /**
     * The Template Method governing the execution flow.
     */
    public final void executeWorkflow(String tenant, int postingDate) throws Throwable {
        ExecutionInstance instance = initializeInstance(tenant, postingDate);
        log.info("Starting execution workflow: instanceId={} tenant={} postingDate={}", instance.getId(), tenant, postingDate);

        try {
            // 1. Pre-Processing
            updateStatus(instance, "PRE_PROCESSING");
            boolean shouldContinue = preProcess(tenant, postingDate);
            if (!shouldContinue) {
                instance.setEndTime(new Date());
                updateStatus(instance, "COMPLETED");
                log.info("Workflow execution halted gracefully by pre-process rules.");
                return;
            }

            // 2. Event Generation and Processing
            updateStatus(instance, "GENERATING_EVENTS");
            generateAndProcessEvents(instance, tenant, postingDate);

            // 3. Post Processing
            updateStatus(instance, "POST_PROCESSING");
            postProcess(instance, tenant, postingDate);

            // 4. End Of Day
            updateStatus(instance, "EOD_PROCESSING");
            performEOD(instance);

            // Complete
            instance.setEndTime(new Date());
            updateStatus(instance, "COMPLETED");

        } catch (Exception e) {
            handleFailure(instance, e);
            throw e;
        }
    }

    private ExecutionInstance initializeInstance(String tenant, int postingDate) {
        ExecutionInstance instance = ExecutionInstance.builder()
                .id(UUID.randomUUID().toString())
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
        executionInstanceRepository.save(instance);
    }

    private void handleFailure(ExecutionInstance instance, Exception e) {
        log.error("Orchestration failed for instance {}: {}", instance.getId(), e.getMessage());
        instance.setStatus("FAILED");
        instance.setErrorMessage(e.getMessage());
        executionInstanceRepository.save(instance);
    }
    
    protected void incrementCompletedBatches(ExecutionInstance instance) {
        instance.setCompletedBatches(instance.getCompletedBatches() == null ? 1 : instance.getCompletedBatches() + 1);
        executionInstanceRepository.save(instance);
    }

    protected abstract String getModelType();
    protected abstract boolean preProcess(String tenant, int postingDate) throws Throwable;
    protected abstract void generateAndProcessEvents(ExecutionInstance instance, String tenant, int postingDate) throws Throwable;
    protected abstract void postProcess(ExecutionInstance instance, String tenant, int postingDate);
    protected abstract void performEOD(ExecutionInstance instance);
}
