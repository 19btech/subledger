package com.reserv.dataloader.service.model.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    // Per run (instance id): the batch job ids whose transactions go into the end-of-run metric
    // roll-up. Filled by generateAndProcessEvents, consumed by postProcess.
    private final Map<String, Set<Long>> metricRollupJobIds = new ConcurrentHashMap<>();

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
                                MetricLevelRollupService metricLevelRollupService) {
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
    protected void generateAndProcessEvents(ExecutionInstance instance, String tenant, int postingDate) throws Throwable {
        Date executionDate = DateUtil.convertToDateFromYYYYMMDD(postingDate);
        AtomicInteger batchCounter = new AtomicInteger(0);

        // Serializes GL sync + progress/failure bookkeeping across concurrently-processing batches —
        // see the lock() call below for why.
        java.util.concurrent.locks.ReentrantLock aggregationLock = new java.util.concurrent.locks.ReentrantLock();
        Set<Long> rollupJobIds = ConcurrentHashMap.newKeySet();
        metricRollupJobIds.put(instance.getId(), rollupJobIds);

        excelModelService.generateEventAndDispatch(postingDate, batch -> {
            int batchNumber = batchCounter.getAndIncrement();
            String correlationId = instance.getId() + "_batch_" + batchNumber;

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
                        // with the rest of the run's in postProcess.
                        rollupJobIds.add(jobResultResponseRecord.jobId());

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
        });
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
        Set<Long> jobIds = metricRollupJobIds.remove(instance.getId());
        try {
            metricLevelRollupService.rollUp(tenant, postingDate, jobIds == null ? Set.of() : jobIds);
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
