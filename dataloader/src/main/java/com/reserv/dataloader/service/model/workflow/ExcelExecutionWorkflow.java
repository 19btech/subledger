package com.reserv.dataloader.service.model.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.ExecutionInstance;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.repository.ExecutionInstanceRepository;
import com.fyntrac.common.service.BatchCompletionWaiter;
import com.fyntrac.common.service.ExcelModelService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.NativeJsonParserService;
import com.fyntrac.common.utils.DateUtil;
import com.reserv.dataloader.pulsar.producer.GeneralLedgerMessageProducer;
import com.reserv.dataloader.service.AggregationExecutionService;
import com.reserv.dataloader.service.model.ModelExecutionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
public class ExcelExecutionWorkflow extends AbstractExecutionWorkflow {

    private final ModelExecutionService modelExecutionService;
    private final ExcelModelService excelModelService;
    private final BatchCompletionWaiter batchCompletionWaiter;
    private final ObjectMapper objectMapper;
    private final AggregationExecutionService aggregationExecutionService;
    private final ExecutionStateService executionStateService;
    private final GeneralLedgerMessageProducer generalLedgerMessageProducer;

    public ExcelExecutionWorkflow(ExecutionInstanceRepository executionInstanceRepository,
                                  ModelExecutionService modelExecutionService,
                                  ExcelModelService excelModelService,
                                  BatchCompletionWaiter batchCompletionWaiter,
                                  ObjectMapper objectMapper,
                                  AggregationExecutionService aggregationExecutionService,
                                  ExecutionStateService executionStateService,
                                  GeneralLedgerMessageProducer generalLedgerMessageProducer) {
        super(executionInstanceRepository);
        this.modelExecutionService = modelExecutionService;
        this.excelModelService = excelModelService;
        this.batchCompletionWaiter = batchCompletionWaiter;
        this.objectMapper = objectMapper;
        this.aggregationExecutionService = aggregationExecutionService;
        this.executionStateService = executionStateService;
        this.generalLedgerMessageProducer = generalLedgerMessageProducer;
    }

    @Override
    protected String getModelType() {
        return "EXCEL";
    }

    @Override
    protected boolean preProcess(String tenant, int postingDate) throws Throwable {
        return modelExecutionService.prepareExcelExecution(postingDate);
    }

    @Override
    protected void generateAndProcessEvents(ExecutionInstance instance, String tenant, int postingDate) throws Throwable {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        Date executionDate = DateUtil.convertToDateFromYYYYMMDD(postingDate);

        AtomicInteger batchCounter = new AtomicInteger(0);

        excelModelService.generateEventAndDispatch(postingDate, batch -> {
            int batchNumber = batchCounter.getAndIncrement();
            String correlationId = instance.getId() + "_batch_" + batchNumber;
            try {
                log.info("Processing Excel batch {} for instance {}. CorrelationId: {}", batchNumber, instance.getId(), correlationId);

                // [Step 2A] Dispatch to Excel Model Service
                modelExecutionService.dispatchExcelBatchOrchestrated(executionDate, batch, correlationId);

                // [Step 2B/C] SUSPEND and WAIT for callback from Model Service
                BatchCompletionWaiter.BatchResult result = batchCompletionWaiter.waitForCompletion(correlationId, 600000L); // 10min timeout

                if (result == null || !"SUCCESS".equals(result.status())) {
                    throw new RuntimeException("Excel processing failed for batch " + batchNumber + ": " +
                            (result != null ? result.errorMessage() : "Null result"));
                }

                // Extract jobId from the result payload JSON
                long excelJobId = -1;
                try {
                    JsonNode root = objectMapper.readTree(result.payload());
                    if (root != null && root.has("jobId")) {
                        excelJobId = root.get("jobId").asLong();
                        log.info("Extracted Excel jobId: {} for correlationId: {}", excelJobId, correlationId);
                    }
                } catch (Exception e) {
                    log.error("Failed to parse jobId from Excel result payload: {}. Payload: {}", e.getMessage(), result.payload());
                }

                // [Step 3] Immediate Financial Aggregation
                Records.JobResultResponseRecord jobResultResponseRecord = NativeJsonParserService.parseJobResultSafely(result.payload());
                Records.ExecuteAggregationMessageRecord executeAggregationMessageRecord = RecordFactory.createExecutionAggregationRecord(tenant,
                        jobResultResponseRecord.jobId(), Long.valueOf(postingDate));
                ExecutionState executionState = executionStateService.getExecutionState();
                aggregationExecutionService.execute(executeAggregationMessageRecord, executionState);

                // [Step 4] Real-Time General Ledger Sync
                Records.GeneralLedgerMessageRecord glRec = RecordFactory.createGeneralLedgerMessageRecord(tenant, jobResultResponseRecord.jobId());
                generalLedgerMessageProducer.bookTempGL(glRec);

                incrementCompletedBatches(instance);
            } catch (Exception e) {
                log.error("Critical failure in Excel batch {} for instance {}: {}", batchNumber, instance.getId(), e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    protected void postProcess(ExecutionInstance instance, String tenant, int postingDate) {
        // Post processing steps for Excel if required in the future
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
                log.info("Excel EOD: Updated lastExecutionDate to {} for tenant {}", instance.getPostingDate(), tenant);
            }
        } catch (Exception e) {
            log.error("Excel EOD Processing failed for instance {}: {}", instance.getId(), e.getMessage());
        } finally {
            // finalizeExcelExecution historically released the lock.
            // Since we released it in finally block of controller, we just write the summary.
            modelExecutionService.finalizeExcelExecution();
        }
    }
}
