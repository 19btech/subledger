package com.fyntrac.model.service;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AccountingPeriodDataUploadService;
import com.fyntrac.common.service.ErrorService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.TransactionActivityService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.model.exception.LoadExcelModelExecption;
import com.fyntrac.model.pulsar.producer.AggregationMessageProducer;
import com.fyntrac.model.pulsar.producer.GeneralLedgerMessageProducer;
import com.fyntrac.model.workflow.ExcelModelExecutor;
import com.fyntrac.model.workflow.ModelExecutionType;
import com.fyntrac.model.workflow.ModelWorkflowContext;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.fyntrac.model.utils.ExcelUtil;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ModelExecutionService {

    private final ModelDataService modelDataService;
    private final MemcachedRepository memcachedRepository;
    private final AccountingPeriodDataUploadService accountingPeriodService;
    private final ExcelModelExecutor excelModelExecutor;
    private final TransactionActivityService transactionActivityService;
    private final ErrorService errorService;
    private final CommonAggregationService commonAggregationService;
    private final GeneralLedgerMessageProducer generalLedgerMessageProducer;
    private final ExecutionStateService executionStateService;
    private final AggregationMessageProducer aggregationMessageProducer;

    public ModelExecutionService(ModelDataService modelDataService
    , MemcachedRepository memcachedRepository
    , AccountingPeriodDataUploadService accountingPeriodService
    , ExcelModelExecutor excelModelExecutor
    , TransactionActivityService transactionActivityService
    , ErrorService errorService
    , CommonAggregationService commonAggregationService
    , GeneralLedgerMessageProducer generalLedgerMessageProducer
    , ExecutionStateService executionStateService
    , AggregationMessageProducer aggregationMessageProducer) {
        this.modelDataService = modelDataService;
        this.memcachedRepository = memcachedRepository;
        this.accountingPeriodService = accountingPeriodService;
        this.excelModelExecutor = excelModelExecutor;
        this.transactionActivityService = transactionActivityService;
        this.errorService = errorService;
        this.commonAggregationService = commonAggregationService;
        this.generalLedgerMessageProducer = generalLedgerMessageProducer;
        this.executionStateService = executionStateService;
        this.aggregationMessageProducer = aggregationMessageProducer;
    }

    public void executeModels(Date executionDate, Records.ModelExecutionMessageRecord msg) throws Throwable {
        try {
            List<Model> models = this.modelDataService.getActiveModels();
            List<Records.ModelRecord> activeModels = new ArrayList<>(0);
            for(Model model : models) {
                ModelFile modelFile = this.modelDataService.getModelFile(model.getModelFileId());
                // Workbook workbook = ExcelUtil.convertBinaryToWorkbook(modelFile.getFileData());
                activeModels.add(RecordFactory.createModelRecord(model, modelFile));
            }

            ExecutionState executionState = this.executionStateService.getExecutionState();
            this.executeModels(executionDate, msg, activeModels, executionState.getActivityPostingDate());
        }catch (Exception exp){
            log.error(StringUtil.getStackTrace(exp));
            throw exp;
        }finally{
            ExecutionState executionState = this.executionStateService.getExecutionState();
            executionState.setLastExecutionDate(executionState.getExecutionDate());
            executionState.setExecutionDate(DateUtil.dateInNumber(executionDate));
            executionStateService.update(executionState);

        }
    }

    public void executeModels(Date executionDate
            , Records.ModelExecutionMessageRecord msg
            , List<Records.ModelRecord> activeModels
    , int previousPostingDate) throws Throwable {
        // Step 1: Validate inputs
        String key = msg.key();
        if (executionDate == null || key == null || activeModels == null || activeModels.isEmpty()) {
            throw new IllegalArgumentException("Invalid input parameters: executionDate, key, or activeModels cannot be null or empty.");
        }

        // Step 2: Camunda Process Engine Initialization

        CacheList<InstrumentAttribute> cacheList = this.memcachedRepository.getFromCache(key, CacheList.class);
        if (cacheList == null || cacheList.getList() == null || cacheList.getList().isEmpty()) {
            log.warn("No instruments found in cache for key: {}", key);
            return; // Exit early if no instruments are found
        }

        int acctPeriod = com.fyntrac.common.utils.DateUtil.getAccountingPeriodId(executionDate);
        AccountingPeriod accountingPeriod = this.accountingPeriodService.getAccountingPeriod(acctPeriod);
        if (accountingPeriod == null) {
            throw new IllegalStateException("Accounting period not found for period ID: " + acctPeriod);
        }

        ExecutionState executionState = this.executionStateService.getExecutionState();
        // Step 3: Virtual Thread Pool
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            String tenantId=msg.tenantId();
            List<Future<?>> futures = new ArrayList<>();

            // Step 4: Process each instrument
            for (InstrumentAttribute ia : cacheList.getList()) {

                futures.add(executor.submit(() -> {
                    try {
                        // Set the tenant context for this virtual thread
                        TenantContextHolder.runWithTenant(tenantId, () -> {
                            try {
                                this.processInstrument(executionDate, accountingPeriod, ia, activeModels, executionState.getActivityPostingDate());
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }
                        });
                    } catch (Throwable e) {
                        log.error("Error processing instrument: {}", ia, e);
                        // throw new RuntimeException("Failed to process instrument: " + ia, e);
                        // save error in database!!!!!

                    }
                }));

            }

            // Step 5: Wait for all tasks to complete
            for (Future<?> future : futures) {
                try {
                    future.get(); // This will rethrow any exceptions thrown in the task
                } catch (ExecutionException e) {
                    // Unwrap the root cause of the exception
                    Throwable cause = e.getCause();
                    log.error("Task execution failed", cause);
                    throw cause; // Rethrow the original exception
                } catch (InterruptedException e) {
                    log.error("Thread interrupted while waiting for tasks to complete", e);
                    Thread.currentThread().interrupt(); // Restore the interrupted status
                    throw new RuntimeException("Thread interrupted", e);
                }
            }
        } catch (Throwable e) {
            log.error("Error in executeModels", e);
            throw e; // Rethrow the exception for further handling
        }
    }

    private void processInstrument(Date executionDate,
                                   AccountingPeriod accountingPeriod
            , InstrumentAttribute instrument
            , List<Records.ModelRecord> activeModels
            , int previousPostingDate) throws Throwable {
        log.info("Processing " + instrument + " on Thread: " + Thread.currentThread().getName());



        // Iterate through each model
        for (Records.ModelRecord model : activeModels) {
            Collection<TransactionActivity> transactionActivities = null;
            try {
                // Build Model workflow context
                ModelWorkflowContext context = ModelWorkflowContext.builder()
                        .currentInstrumentAttribute(instrument)
                        .instrumentId(instrument.getInstrumentId())
                        .attributeId(instrument.getAttributeId())
                        .executionType(ModelExecutionType.CHAINED)
                        .accountingPeriod(accountingPeriod)
                        .executionDate(executionDate)
                        .excelModel(model).build();

                // Execute the model
                Records.ModelOutputData modelOutputData = this.excelModelExecutor.execute(context);
                    // Save transaction activities
                    Set<TransactionActivity> transactions = transactionActivityService.generateTransactions(
                            modelOutputData.transactionActivityList(),
                            instrument,
                            accountingPeriod,
                            executionDate,
                            Source.MODEL,
                            model.model().getId()
                    );
                    // Filter out TransactionActivity where amount is zero
                    Set<TransactionActivity> filteredTransactions = transactions.stream()
                            .filter(transaction -> transaction.getAmount().compareTo(BigDecimal.ZERO) != 0)
                            .collect(Collectors.toSet());
                    transactionActivities = this.transactionActivityService.save(filteredTransactions);

                    // Now send message to generate GL
                    // Now send message to Aggregate transactions
                    // this.commonAggregationService.aggregate(transactionActivities, previousPostingDate);
                    LocalDateTime dateTime = LocalDateTime.now();
                    int timestamp = (int) (dateTime.toEpochSecond(ZoneOffset.UTC));
                    String tenantId = this.transactionActivityService.getDataService().getTenantId();
                    String key = String.format("%s-%s", tenantId, "TA");

                    String transactionActivityKey = String.format("%s-%d", tenantId, timestamp);

                    TransactionActivityList activityList = new TransactionActivityList();
                    if (transactionActivities != null) {
                        transactionActivities.forEach(transactionActivity -> {
                                    String activityKey = String.format("%s-%d", key, transactionActivity.hashCode());

                                    activityList.add(activityKey);
                                    this.memcachedRepository.putInCache(activityKey, transactionActivity);
                                }
                        );
                    }

                    this.memcachedRepository.putInCache(transactionActivityKey, activityList);
                    Records.ExecuteAggregationMessageRecord aggregationMessageRecord = RecordFactory.createExecutionAggregationRecord(tenantId, transactionActivityKey, (long) DateUtil.dateInNumber(executionDate));
                    aggregationMessageProducer.executeAggregation(aggregationMessageRecord);

            } catch (Exception e) {
                // Log the exception and continue processing the next model
                log.error("An error occurred while processing model " + model.model().getId() + " for instrument " + instrument.getInstrumentId(), e);
                // Optionally, you can add additional handling here (e.g., notify, retry, etc.)
                Errors error = Errors.builder().modelId(model.model().getId())
                        .instrumentId(instrument.getInstrumentId())
                        .attributeId(instrument.getAttributeId())
                        .executionDate(executionDate)
                        .code(ErrorCode.Model_Execution_Error)
                        .isWarning(Boolean.FALSE)
                        .stacktrace(StringUtil.getStackTrace(e))
                        .build();
                this.errorService.save(error);
                throw e;
            }finally {

                    LocalDateTime dateTime = LocalDateTime.now();
                    int timestamp = (int) (dateTime.toEpochSecond(ZoneOffset.UTC));
                    String tenantId = this.transactionActivityService.getDataService().getTenantId();
                    String key = String.format("%s-%d", tenantId, timestamp);

                    String transactionActivityKey = String.format("%s-%d", key, timestamp);

                    TransactionActivityList activityList = new TransactionActivityList();
                    if (transactionActivities != null) {
                        transactionActivities.forEach(transactionActivity -> {
                                    String activityKey = String.format("%s-%d", key, transactionActivity.hashCode());

                                    activityList.add(activityKey);
                                    this.memcachedRepository.putInCache(activityKey, transactionActivity);
                                }
                        );
                    }

                    this.memcachedRepository.putInCache(transactionActivityKey, activityList);


                    Records.GeneralLedgerMessageRecord glRec = RecordFactory.createGeneralLedgerMessageRecord(this.transactionActivityService.getDataService().getTenantId(), transactionActivityKey);
                    generalLedgerMessageProducer.bookTempGL(glRec);
            }
        }
    }
    public Map<Integer, Records.ExcelModelRecord> loadModels(List<Records.ModelRecord> activeModels) throws Exception {
        Map<Integer, Records.ExcelModelRecord> excelModelsMap = new HashMap<>();
        int priority = 1;

        for (Records.ModelRecord modelRecord : activeModels) {
            try {
                Records.ExcelModelRecord excelModel = RecordFactory.createExcelModelRecord(
                        modelRecord.model(),
                        ExcelUtil.convertBinaryToWorkbook(modelRecord.modelFile().getFileData())
                );
                excelModelsMap.put(priority++, excelModel);
            } catch (Exception e) {
                log.error("Failed to load model: " + modelRecord, e);
                throw new LoadExcelModelExecption("Failed to load model: [" + modelRecord + "]"
                        +  StringUtil.getStackTrace(e));
            }
        }

        return excelModelsMap;
    }

}
