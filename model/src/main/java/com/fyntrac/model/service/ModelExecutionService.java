package com.fyntrac.model.service;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AccountingPeriodDataUploadService;
import com.fyntrac.model.exception.LoadExcelModelExecption;
import com.fyntrac.model.workflow.ExcelModelExecutor;
import com.fyntrac.model.workflow.ModelExecutionType;
import com.fyntrac.model.workflow.ModelWorkflowContext;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.entity.Model;
import com.fyntrac.common.entity.ModelFile;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fyntrac.model.utils.ExcelUtil;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ModelExecutionService {

    private final ModelDataService modelDataService;
    private final MemcachedRepository memcachedRepository;
    private final AccountingPeriodDataUploadService accountingPeriodService;
    private final ExcelModelExecutor excelModelExecutor;
    public ModelExecutionService(ModelDataService modelDataService
    , MemcachedRepository memcachedRepository
    , AccountingPeriodDataUploadService accountingPeriodService
    , ExcelModelExecutor excelModelExecutor) {
        this.modelDataService = modelDataService;
        this.memcachedRepository = memcachedRepository;
        this.accountingPeriodService = accountingPeriodService;
        this.excelModelExecutor = excelModelExecutor;
    }

    public void executeModels(Date executionDate, Records.CommonMessageRecord msg) throws Throwable {
        try {
            List<Model> models = this.modelDataService.getActiveModels();
            List<Records.ModelRecord> activeModels = new ArrayList<>(0);
            for(Model model : models) {
                ModelFile modelFile = this.modelDataService.getModelFile(model.getModelFileId());
                // Workbook workbook = ExcelUtil.convertBinaryToWorkbook(modelFile.getFileData());
                activeModels.add(RecordFactory.createModelRecord(model, modelFile));
            }

            this.executeModels(executionDate, msg, activeModels);
        }catch (Exception exp){
            log.error(StringUtil.getStackTrace(exp));
            throw exp;
        }
    }

    public void executeModels(Date executionDate, Records.CommonMessageRecord msg, List<Records.ModelRecord> activeModels) throws Throwable {
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
                                this.processInstrument(executionDate, accountingPeriod, ia, activeModels);
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

    // Step 5: Processing an instrument and starting a Camunda workflow
    private void processInstrument(Date executionDate, AccountingPeriod accountingPeriod, InstrumentAttribute instrument, List<Records.ModelRecord> activeModels) throws Throwable {
        log.info("Processing " + instrument + " on Thread: " + Thread.currentThread().getName());

        // Start a model workflow
        // build Model workflow context
        // Map<Integer, Records.ExcelModelRecord> excelModels = this.loadModels(activeModels);
        for(Records.ModelRecord model : activeModels) {
            ModelWorkflowContext context = ModelWorkflowContext.builder()
                    .currentInstrumentAttribute(instrument)
                    .instrumentId(instrument.getInstrumentId())
                    .attributeId(instrument.getAttributeId())
                    .executionType(ModelExecutionType.CHAINED)
                    .accountingPeriod(accountingPeriod)
                    .executionDate(executionDate)
                    .excelModel(model).build();
            this.excelModelExecutor.execute(context);
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
