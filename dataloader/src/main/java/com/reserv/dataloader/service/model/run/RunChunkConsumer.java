package com.reserv.dataloader.service.model.run;

import com.reserv.dataloader.service.model.workflow.DslExecutionWorkflow;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Makes this pod a worker for distributed runs: takes chunks off {@link RunChunkQueue} and processes
 * them through {@link DslExecutionWorkflow#processChunk}. Started only once the application is ready,
 * so a pod never takes a chunk before it can serve it.
 */
@Slf4j
@Component
public class RunChunkConsumer {

    // Chunks one pod works on at once. Each chunk pages and dispatches independently; they share the
    // pod-wide dispatch and instrument-group limits (ExcelModelService), so raising this adds overlap,
    // not unbounded load.
    @Value("${fyntrac.run.chunks-per-pod:2}")
    private int chunksPerPod;

    private final DistributedRunService distributedRunService;
    private final RunChunkQueue runChunkQueue;
    private final DslExecutionWorkflow dslExecutionWorkflow;

    public RunChunkConsumer(DistributedRunService distributedRunService, RunChunkQueue runChunkQueue,
                            DslExecutionWorkflow dslExecutionWorkflow) {
        this.distributedRunService = distributedRunService;
        this.runChunkQueue = runChunkQueue;
        this.dslExecutionWorkflow = dslExecutionWorkflow;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() throws Exception {
        if (!distributedRunService.isEnabled()) {
            log.info("Distributed runs disabled (fyntrac.run.distributed=false); not consuming run chunks");
            return;
        }
        runChunkQueue.startConsumers(Math.max(1, chunksPerPod), dslExecutionWorkflow::processChunk);
    }
}
