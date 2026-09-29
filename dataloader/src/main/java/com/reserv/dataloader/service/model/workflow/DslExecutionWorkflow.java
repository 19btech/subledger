package com.reserv.dataloader.service.model.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.Errors;
import com.fyntrac.common.entity.ExecutionInstance;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.enums.ErrorCategory;
import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.enums.ErrorType;
import com.fyntrac.common.repository.ExecutionInstanceRepository;
import com.fyntrac.common.service.BatchCompletionWaiter;
import com.fyntrac.common.service.ErrorService;
import com.fyntrac.common.service.ExcelModelService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.NativeJsonParserService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.StringUtil;
import com.reserv.dataloader.pulsar.producer.GeneralLedgerMessageProducer;
import com.reserv.dataloader.pulsar.producer.PythonModelExecutionProducer;
import com.reserv.dataloader.service.AggregationExecutionService;
import com.reserv.dataloader.service.MetricLevelRollupService;
import com.reserv.dataloader.service.model.ModelExecutionService;
import com.reserv.dataloader.service.model.run.DistributedRunService;
import com.reserv.dataloader.service.model.run.RunChunkMessage;
import com.reserv.dataloader.service.model.run.RunChunkQueue;
import com.reserv.dataloader.service.model.run.TenantExecutionLock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
public class DslExecutionWorkflow extends AbstractExecutionWorkflow {

    private final ModelExecutionService modelExecutionService;
    private final ExcelModelService excelModelService;
    private final PythonModelExecutionProducer pythonModelExecutionProducer;
    private final BatchCompletionWaiter batchCompletionWaiter;
    private final ObjectMapper objectMapper;
    private final AggregationExecutionService aggregationExecutionService;
    private final ExecutionStateService executionStateService;
    private final GeneralLedgerMessageProducer generalLedgerMessageProducer;
    private final ErrorService errorService;
    private final MetricLevelRollupService metricLevelRollupService;
    private final DistributedRunService distributedRunService;
    private final TenantExecutionLock tenantExecutionLock;

    public DslExecutionWorkflow(ExecutionInstanceRepository executionInstanceRepository,
                                ModelExecutionService modelExecutionService,
                                ExcelModelService excelModelService,
                                PythonModelExecutionProducer pythonModelExecutionProducer,
                                BatchCompletionWaiter batchCompletionWaiter,
                                ObjectMapper objectMapper,
                                AggregationExecutionService aggregationExecutionService,
                                ExecutionStateService executionStateService,
                                GeneralLedgerMessageProducer generalLedgerMessageProducer,
                                ErrorService errorService,
                                MetricLevelRollupService metricLevelRollupService,
                                DistributedRunService distributedRunService,
                                TenantExecutionLock tenantExecutionLock) {
        super(executionInstanceRepository);
        this.modelExecutionService = modelExecutionService;
        this.excelModelService = excelModelService;
        this.pythonModelExecutionProducer = pythonModelExecutionProducer;
        this.batchCompletionWaiter = batchCompletionWaiter;
        this.objectMapper = objectMapper;
        this.aggregationExecutionService = aggregationExecutionService;
        this.executionStateService = executionStateService;
        this.generalLedgerMessageProducer = generalLedgerMessageProducer;
        this.errorService = errorService;
        this.metricLevelRollupService = metricLevelRollupService;
        this.distributedRunService = distributedRunService;
        this.tenantExecutionLock = tenantExecutionLock;
    }

    @Override
    protected String getModelType() {
        return "DSL";
    }

    @Override
    protected boolean preProcess(String tenant, int postingDate) throws Throwable {
        return modelExecutionService.preparePythonExecution(postingDate);
    }

    @Override
    protected boolean tailRunsElsewhere() {
        return distributedRunService.isEnabled();
    }

    @Override
    protected void generateAndProcessEvents(ExecutionInstance instance, String tenant, int postingDate) throws Throwable {
        if (distributedRunService.isEnabled()) {
            startDistributedRun(instance, tenant, postingDate);
            return;
        }
        BatchProcessor batches = new BatchProcessor(instance, tenant, postingDate, "");
        excelModelService.generateEventAndDispatch(postingDate, batches::process);
    }

    // ── Distributed run (TARGET_DESIGN_DISTRIBUTED_RUN.md) ─────────────────────
    // The coordinator — this pod — writes the shared reference events, cuts the instruments into
    // chunks and publishes them. Any dataloader pod (this one included) takes chunks and, for its
    // instruments only, generates events, dispatches batches to the model workers and runs the
    // per-batch roll-up and GL sync — the same BatchProcessor as a single-pod run. The pod whose
    // chunk completes the run runs the tail: metric roll-up, EOD, final status.

    private void startDistributedRun(ExecutionInstance instance, String tenant, int postingDate) throws Exception {
        boolean sharedReferenceEventsExist = excelModelService.saveSharedReferenceEvents(postingDate);
        List<RunChunkMessage> chunks = distributedRunService.plan(tenant, instance.getId(), postingDate,
                sharedReferenceEventsExist);
        if (chunks.isEmpty()) {
            // Nothing to hand out; the tail (carry-forward, date advance) still runs.
            if (distributedRunService.tryFinalize(tenant, instance.getId())) {
                finishRun(tenant, instance.getId());
            }
            return;
        }
        distributedRunService.publish(chunks);
    }

    /** Handles one chunk of a distributed run, on whichever pod received it. */
    public RunChunkQueue.Outcome processChunk(RunChunkMessage chunk) {
        return TenantContextHolder.runWithTenant(chunk.tenantId(), () -> processChunkInTenant(chunk));
    }

    private RunChunkQueue.Outcome processChunkInTenant(RunChunkMessage chunk) {
        String tenant = chunk.tenantId();
        switch (distributedRunService.claim(chunk)) {
            case ALREADY_FINISHED:
                return RunChunkQueue.Outcome.ACK;
            case OWNED_ELSEWHERE:
                return RunChunkQueue.Outcome.RETRY_LATER;
            case OWNER_LOST: {
                // Its batches may have been partly processed (transactions written, some rolled up
                // and booked). Re-running the chunk would count those twice, so it is failed: the run
                // ends PARTIAL_SUCCESS and the posting date needs a re-run, which cleans it first.
                String error = "Chunk " + chunk.seq() + " was abandoned by the pod processing it; its instruments ("
                        + chunk.fromExclusive() + ", " + chunk.toInclusive() + "] may be partially processed "
                        + "- re-run the posting date";
                executionInstanceRepository.findById(chunk.runId()).ifPresent(instance ->
                        recordBatchFailure(tenant, instance, -1, chunk.runId() + "_chunk_" + chunk.seq(),
                                new IllegalStateException(error)));
                if (distributedRunService.finishChunk(chunk, false, true, error)) {
                    finishRun(tenant, chunk.runId());
                }
                return RunChunkQueue.Outcome.ACK;
            }
            default:
                break;
        }

        ExecutionInstance instance = executionInstanceRepository.findById(chunk.runId()).orElse(null);
        boolean failed = false;
        String error = null;
        long startNanos = System.nanoTime();
        if (instance == null) {
            failed = true;
            error = "Run " + chunk.runId() + " not found";
            log.error("Chunk {}: {}", chunk.chunkId(), error);
        } else {
            try {
                BatchProcessor batches = new BatchProcessor(instance, tenant, chunk.postingDate(), "c" + chunk.seq() + "_");
                excelModelService.generateEventAndDispatch(chunk.postingDate(), chunk.fromExclusive(),
                        chunk.toInclusive(), chunk.sharedReferenceEventsExist(), batches::process);
            } catch (Exception e) {
                failed = true;
                error = e.getMessage();
                recordBatchFailure(tenant, instance, -1, chunk.runId() + "_chunk_" + chunk.seq(), e);
            }
        }
        log.info("Run {} chunk {} processed on {} in {} ms{}", chunk.runId(), chunk.seq(),
                distributedRunService.podName(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos),
                failed ? " (FAILED: " + error + ")" : "");
        if (distributedRunService.finishChunk(chunk, true, failed, error)) {
            finishRun(tenant, chunk.runId());
        }
        return RunChunkQueue.Outcome.ACK;
    }

    // Runs the tail on the pod that won the run's finalization, then frees the tenant's lock.
    private void finishRun(String tenant, String runId) {
        ExecutionInstance instance = executionInstanceRepository.findById(runId).orElse(null);
        if (instance == null) {
            log.error("Run {} not found when finishing it", runId);
            return;
        }
        log.info("Finishing run {} (tenant {}, postingDate {}) on {}", runId, tenant, instance.getPostingDate(),
                distributedRunService.podName());
        completeRun(instance);
        tenantExecutionLock.releaseForRun(tenant, runId);
    }

    /**
     * Per-batch work for one run (single-pod) or one chunk (distributed): dispatch to the model
     * workers, await the result, instrument-scoped roll-up, then GL sync and progress bookkeeping.
     */
    private final class BatchProcessor {
        private final ExecutionInstance instance;
        private final String tenant;
        private final int postingDate;
        private final Date executionDate;
        // Distinguishes batch correlation ids across the chunks of one run.
        private final String batchPrefix;
        private final AtomicInteger batchCounter = new AtomicInteger(0);
        // Serializes GL sync + progress/failure bookkeeping across concurrently-processing batches —
        // see the lock() call below for why.
        private final java.util.concurrent.locks.ReentrantLock aggregationLock = new java.util.concurrent.locks.ReentrantLock();

        BatchProcessor(ExecutionInstance instance, String tenant, int postingDate, String batchPrefix) {
            this.instance = instance;
            this.tenant = tenant;
            this.postingDate = postingDate;
            this.executionDate = DateUtil.convertToDateFromYYYYMMDD(postingDate);
            this.batchPrefix = batchPrefix;
        }

        void process(Set<String> batch) {
            int batchNumber = batchCounter.getAndIncrement();
            String correlationId = instance.getId() + "_batch_" + batchPrefix + batchNumber;

            // Dispatch-to-Python + awaitCompletion (the genuinely slow part) run outside the lock,
            // fully concurrently across batches, same as before. Any failure here — dispatch,
            // timeout, a Python-side error, a malformed payload — is captured instead of thrown,
            // so one bad batch can no longer abort the other 48 via this callback's exception.
            Exception batchFailure = null;
            Records.JobResultResponseRecord jobResultResponseRecord = null;
            try {
                log.info("Processing batch {} for instance {}. CorrelationId: {}", batchNumber, instance.getId(), correlationId);

                // [Step 2A] Dispatch to Python
                dispatchPythonBatchOrchestrated(executionDate, batch, correlationId, tenant);

                // [Step 2B] SUSPEND and WAIT for callback (polls Memcached — see BatchCompletionWaiter).
                // Runs on this batch's own virtual thread, concurrently with every other batch's wait.
                BatchCompletionWaiter.BatchResult result = batchCompletionWaiter.awaitCompletion(correlationId, 600000L); // 10min timeout

                if (result == null || !"SUCCESS".equals(result.status())) {
                    throw new RuntimeException("Python processing failed for batch " + batchNumber + ": " +
                            (result != null ? result.errorMessage() : "Null result"));
                }

                // Extract jobId from the result payload JSON
                try {
                    JsonNode root = objectMapper.readTree(result.payload());
                    if (root != null && root.has("jobId")) {
                        log.info("Extracted Python jobId: {} for correlationId: {}", root.get("jobId").asLong(), correlationId);
                    }
                } catch (Exception e) {
                    log.error("Failed to parse jobId from Python result payload: {}. Payload: {}", e.getMessage(), result.payload());
                }

                jobResultResponseRecord = NativeJsonParserService.parseJobResultSafely(result.payload());

                // [Step 3a] Attribute- and instrument-level aggregation. Every row these jobs
                // touch is keyed by instrument, and an instrument is in exactly one batch, so
                // this is safe to run concurrently with other batches — and it's the part of
                // aggregation whose cost grows with instrument count, so it must not sit behind
                // the lock below.
                aggregationExecutionService.executeInstrumentScoped(
                        RecordFactory.createExecutionAggregationRecord(tenant, jobResultResponseRecord.jobId(), Long.valueOf(postingDate)),
                        executionStateService.getExecutionState());
            } catch (Exception e) {
                batchFailure = e;
            }

            // Serializes GL sync + progress/failure bookkeeping across concurrently-processing
            // batches (generateEventAndDispatch runs each batch's callback on its own virtual thread
            // — see its own comment): incrementCompletedBatches/incrementFailedBatches
            // read-modify-write the shared `instance` object. Metric-level aggregation no longer
            // runs here — it used to, and was the reason this lock existed, because every batch
            // read-modify-wrote the same (metric, postingDate) MetricLevelLtd rows. It now runs
            // once for the whole run in postProcess (MetricLevelRollupService).
            aggregationLock.lock();
            try {
                if (batchFailure == null) {
                    try {
                        // [Step 3b] Metric-level aggregation: this batch's transactions are rolled up
                        // with the rest of the run's in postProcess. Recorded in MongoDB, not in this
                        // pod's memory, so any pod can run the roll-up.
                        metricLevelRollupService.recordRunBatch(tenant, instance.getId(), postingDate,
                                jobResultResponseRecord.jobId());

                        // [Step 4] Real-Time General Ledger Sync
                        Records.GeneralLedgerMessageRecord glRec = RecordFactory.createGeneralLedgerMessageRecord(tenant, jobResultResponseRecord.jobId());
                        generalLedgerMessageProducer.bookTempGL(glRec);
                    } catch (Exception e) {
                        batchFailure = e;
                    }
                }

                if (batchFailure != null) {
                    // Logged + persisted to the Errors collection, but deliberately not rethrown:
                    // one batch's failure shouldn't abort the other batches of this DSL run. The
                    // instance still ends up PARTIAL_SUCCESS (not a silent COMPLETED) — see
                    // AbstractExecutionWorkflow's final status check.
                    recordBatchFailure(tenant, instance, batchNumber, correlationId, batchFailure);
                    incrementFailedBatches(instance);
                }

                // Update instance progress — every batch reaches this point exactly once, whether
                // it succeeded or failed, so completion tracking/UI progress never stalls.
                incrementCompletedBatches(instance);
            } finally {
                aggregationLock.unlock();
            }
        }
    }

    /**
     * Logs a single batch's failure and persists it to the Errors collection so it survives past
     * this JVM's log retention — same reasoning as AggregationExecutionService.recordJobFailure.
     * Never throws: a problem persisting the error must not itself abort the batch.
     */
    private void recordBatchFailure(String tenant, ExecutionInstance instance, int batchNumber, String correlationId, Exception e) {
        log.error("Batch {} failed for instance {} (tenant {}), correlationId {}: {}",
                batchNumber, instance.getId(), tenant, correlationId, e.getMessage(), e);

        try {
            Errors error = Errors.builder()
                    .jobId(correlationId)
                    .modelId(instance.getId())
                    .postingDate(DateUtil.convertToDateFromYYYYMMDD(instance.getPostingDate()))
                    .executionDate(DateUtil.convertToDateFromYYYYMMDD(instance.getPostingDate()))
                    .code(ErrorCode.Event_Generation_Error)
                    .errorCategory(ErrorCategory.PROCESSING)
                    .errorType(ErrorType.ERROR)
                    .isWarning(Boolean.FALSE)
                    .message("Batch " + batchNumber + " failed for tenant " + tenant + ": " + e.getMessage())
                    .stacktrace(StringUtil.getStackTrace(e))
                    .build();
            errorService.save(error);
        } catch (Exception persistError) {
            log.error("Failed to persist batch {} failure to Errors collection for instance {}: {}",
                    batchNumber, instance.getId(), persistError.getMessage(), persistError);
        }
    }

    private void dispatchPythonBatchOrchestrated(Date executionDate, Set<String> instrumentIds, String correlationId, String tenantId) {
        if (instrumentIds == null || instrumentIds.isEmpty()) return;

        this.pythonModelExecutionProducer.sendPythonModelExecutionMessageOrchestrated(
                RecordFactory.createPythonModelExecutionMessage(tenantId, DateUtil.dateInNumber(executionDate), new ArrayList<>(instrumentIds), false),
                correlationId);

        log.debug("Orchestrated dispatch: batch correlationId={} instrumentsCount={}", correlationId, instrumentIds.size());
    }

    @Override
    protected void postProcess(ExecutionInstance instance, String tenant, int postingDate) {
        // Metric-level LTD for the whole run, after every batch has finished and before
        // performEOD's post-aggregation carry-forward (which must see this posting date's rows,
        // or it treats every metric as inactive and writes a zero row for it).
        try {
            metricLevelRollupService.rollUp(tenant, postingDate, instance.getId());
        } catch (Exception e) {
            // Same outcome as a batch whose metric step failed used to have: recorded, and the
            // instance ends PARTIAL_SUCCESS rather than a silent COMPLETED.
            recordBatchFailure(tenant, instance, -1, instance.getId() + "_metric_rollup", e);
            incrementFailedBatches(instance);
        }
    }

    @Override
    protected void performEOD(ExecutionInstance instance) {
        log.info("Performing EOD processing for instanceId={} tenant={}", instance.getId(), instance.getTenantId());
        try {
            String tenant = instance.getTenantId();
            ExecutionState state = executionStateService.getExecutionState();
            Records.ExecuteAggregationMessageRecord executeAggregationMessageRecord = RecordFactory.createExecutionAggregationRecord(tenant,
                    0, instance.getPostingDate().longValue());

            aggregationExecutionService.executePostAggregation(executeAggregationMessageRecord, state);
            if (state != null) {
                if(instance.getPostingDate() > state.getExecutionDate()) {
                    state.setLastExecutionDate(state.getExecutionDate());
                    state.setExecutionDate(instance.getPostingDate());
                    executionStateService.update(state);
                }
                log.info("EOD: Updated lastExecutionDate to {} for tenant {}", instance.getPostingDate(), tenant);
            }
        } catch (Exception e) {
            log.error("EOD Processing failed for instance {}: {}", instance.getId(), e.getMessage());
        }
    }
}
