package com.fyntrac.model.service;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.component.TransactionActivityQueue;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.*;
import com.fyntrac.common.model.ExcelModelExecutor;
import com.fyntrac.common.model.ModelWorkflowContext;
import com.fyntrac.common.repository.EventRepository;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.*;
import com.fyntrac.common.service.aggregation.CommonAggregationService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.StringUtil;
import com.fyntrac.model.pulsar.producer.AggregationMessageProducer;
import com.fyntrac.model.pulsar.producer.GeneralLedgerMessageProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
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
    private final InstrumentAttributeService instrumentAttributeService;
    private final TransactionActivityQueue transactionActivityQueue;
    private final EventRepository eventRepository;
    public ModelExecutionService(ModelDataService modelDataService
    , MemcachedRepository memcachedRepository
    , AccountingPeriodDataUploadService accountingPeriodService
    , ExcelModelExecutor excelModelExecutor
    , TransactionActivityService transactionActivityService
    , ErrorService errorService
    , CommonAggregationService commonAggregationService
    , GeneralLedgerMessageProducer generalLedgerMessageProducer
    , ExecutionStateService executionStateService
    , AggregationMessageProducer aggregationMessageProducer
    , InstrumentAttributeService instrumentAttributeService
    , TransactionActivityQueue transactionActivityQueue
    , EventRepository eventRepository) {
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
        this.instrumentAttributeService = instrumentAttributeService;
        this.transactionActivityQueue = transactionActivityQueue;
        this.eventRepository = eventRepository;

    }
    public void executeExcelModels(Date executionDate, Records.ModelExecutionMessageRecord msg) throws Throwable {
        try {
            List<Model> models = this.modelDataService.getActiveModels(msg.tenantId());
            // this.eventRepository.findAllByPostingDate(Date)
            List<Records.ModelRecord> activeModels = new ArrayList<>(0);
            for(Model model : models) {
                ModelFile modelFile = this.modelDataService.getModelFile(model.getModelFileId(), msg.tenantId());
                // Workbook workbook = ExcelUtil.convertBinaryToWorkbook(modelFile.getFileData());
                activeModels.add(RecordFactory.createModelRecord(model, modelFile));
            }

            ExecutionState executionState = this.executionStateService.getExecutionState();
            if(executionState == null) {
                executionState = ExecutionState.builder()
                        .executionDate(0)
                        .build();

            }

            // Step 2: Camunda Process Engine Initialization
            int maxRetries = 5;
            int attempts = 0;
            CacheList<String> cacheList = null;
            String key = msg.key();
            while (attempts < maxRetries) {
                attempts++;
                try {
                    cacheList = this.memcachedRepository.getFromCache(key, CacheList.class);
                    if (cacheList != null && cacheList.getList() != null && !cacheList.getList().isEmpty()) {
                        break; // Successfully fetched non-empty list, exit retry loop
                    }
                    log.warn("Attempt {}: Cache miss or empty list for key: {}", attempts, key);
                } catch (Exception e) {
                    log.error("Attempt {}: Error accessing cache for key {}: {}", attempts, key, e.getMessage());
                }

                // Optional: Add a delay between retries
                try {
                    Thread.sleep(100); // 100ms delay between retries
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); // Restore interrupted state
                    break;
                }
            }

            // Now send message to Aggregate transactions
            // this.commonAggregationService.aggregate(transactionActivities, previousPostingDate);
            LocalDateTime dateTime = LocalDateTime.now();
            int timestamp = (int) (dateTime.toEpochSecond(ZoneOffset.UTC));
            String tenantId=msg.tenantId();
            int postingDate = DateUtil.dateInNumber(executionDate);
            String jobKey = String.format("%s-%s-%d", tenantId, "TA", postingDate);
            long jobId = System.currentTimeMillis();
            String transactionActivityKey = String.format("%s-%d", jobKey, timestamp);
            TransactionActivityList activityList = new TransactionActivityList();
            // Step 3: Virtual Thread Pool
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

                Integer lastActivityPostingDate = this.transactionActivityService.getLatestActivityPostingDate(tenantId);
                List<Future<?>> futures = new ArrayList<>();


                int acctPeriod = com.fyntrac.common.utils.DateUtil.getAccountingPeriodId(executionDate);
                AccountingPeriod accountingPeriod = this.accountingPeriodService.getAccountingPeriod(acctPeriod);

                // Step 4: Process each instrument
                for (String instrumentId : cacheList.getList()) {

                    List<Event> events = this.eventRepository.findByPostingDateAndInstrumentId(postingDate, instrumentId);
                    futures.add(executor.submit(() -> {
                        try {
                            // Set the tenant context for this virtual thread
                            TenantContextHolder.runWithTenant(tenantId, () -> {
                                try {
                                  Collection<TransactionActivity> activities = this.processInstrument(tenantId,
                                            postingDate,
                                           accountingPeriod
            , instrumentId
            , activeModels
            , events);

                                    if (activities != null) {
                                        activities.forEach(transactionActivity -> {
                                            log.info(String.format("Adding TransactionActivityQueue[%s]",
                                                    transactionActivity.toString()));
                                                    this.transactionActivityQueue.add(tenantId, jobId, transactionActivity);
                                            String activityKey = String.format("%s-%d", key, transactionActivity.hashCode());

                                            activityList.add(activityKey);
                                            this.memcachedRepository.putInCache(activityKey, transactionActivity);
                                                }
                                        );
                                    }
                                } catch (Throwable e) {
                                    throw new RuntimeException(e);
                                }
                            });
                        } catch (Throwable e) {
                            log.error("Error processing instrument: {}", instrumentId, e);
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
            }finally {
                Records.ExecuteAggregationMessageRecord aggregationMessageRecord =
                        RecordFactory.createExecutionAggregationRecord(tenantId, jobId, (long) postingDate);
                aggregationMessageProducer.executeAggregation(aggregationMessageRecord);
                this.memcachedRepository.putInCache(transactionActivityKey, activityList);
                Records.GeneralLedgerMessageRecord glRec = RecordFactory.createGeneralLedgerMessageRecord(this.transactionActivityService.getDataService().getTenantId(), jobId);
                generalLedgerMessageProducer.bookTempGL(glRec);
            }
        }catch (Exception exp){
            log.error(StringUtil.getStackTrace(exp));
            throw exp;
        }
    }

    public void executeModels(Date executionDate, Records.ModelExecutionMessageRecord msg) throws Throwable {
        try {
            List<Model> models = this.modelDataService.getActiveModels(msg.tenantId());
            List<Records.ModelRecord> activeModels = new ArrayList<>(0);
            for(Model model : models) {
                ModelFile modelFile = this.modelDataService.getModelFile(model.getModelFileId(), msg.tenantId());
                // Workbook workbook = ExcelUtil.convertBinaryToWorkbook(modelFile.getFileData());
                activeModels.add(RecordFactory.createModelRecord(model, modelFile));
            }

            ExecutionState executionState = this.executionStateService.getExecutionState();
            if(executionState == null) {
                executionState = ExecutionState.builder()
                        .executionDate(0)
                        .build();

            }
            this.executeModels(executionDate, msg, activeModels, executionState.getExecutionDate());
            if(executionState.getExecutionDate() < DateUtil.dateInNumber(executionDate)) {
                executionState.setLastExecutionDate(executionState.getExecutionDate());
            }

            executionState.setExecutionDate(DateUtil.dateInNumber(executionDate));
            executionStateService.update(executionState);

        }catch (Exception exp){
            log.error(StringUtil.getStackTrace(exp));
            throw exp;
        }finally{
//            ExecutionState executionState = this.executionStateService.getExecutionState();
//            executionState.setLastExecutionDate(executionState.getExecutionDate());
//            executionState.setExecutionDate(DateUtil.dateInNumber(executionDate));
//            executionStateService.update(executionState);

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
        int maxRetries = 5;
        int attempts = 0;
        CacheList<String> cacheList = null;

        while (attempts < maxRetries) {
            attempts++;
            try {
                cacheList = this.memcachedRepository.getFromCache(key, CacheList.class);
                if (cacheList != null && cacheList.getList() != null && !cacheList.getList().isEmpty()) {
                    break; // Successfully fetched non-empty list, exit retry loop
                }
                log.warn("Attempt {}: Cache miss or empty list for key: {}", attempts, key);
            } catch (Exception e) {
                log.error("Attempt {}: Error accessing cache for key {}: {}", attempts, key, e.getMessage());
            }

            // Optional: Add a delay between retries
            try {
                Thread.sleep(100); // 100ms delay between retries
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt(); // Restore interrupted state
                break;
            }
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
            Integer lastActivityPostingDate = this.transactionActivityService.getLatestActivityPostingDate(tenantId);
            List<Future<?>> futures = new ArrayList<>();

            // Step 4: Process each instrument
            for (String instrumentId : cacheList.getList()) {

                futures.add(executor.submit(() -> {
                    try {
                        // Set the tenant context for this virtual thread
                        TenantContextHolder.runWithTenant(tenantId, () -> {
                            try {
                                if(executionState == null ||  executionState.getExecutionDate() == null){
                                    this.processInstrument(tenantId, executionDate, accountingPeriod, instrumentId, activeModels, 0, lastActivityPostingDate);
                                }else {
                                    this.processInstrument(tenantId, executionDate, accountingPeriod, instrumentId, activeModels, executionState.getExecutionDate(), lastActivityPostingDate);
                                }
                                } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }
                        });
                    } catch (Throwable e) {
                        log.error("Error processing instrument: {}", instrumentId, e);
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

    private Collection<TransactionActivity> processInstrument(String tenantId, int executionDate,
                                   AccountingPeriod accountingPeriod
            , String instrumentId
            , List<Records.ModelRecord> activeModels
            , List<Event> events) throws Exception {
        log.info("Processing " + instrumentId + " on Thread: " + Thread.currentThread().getName());

        List<InstrumentAttribute> currentOpenInstrumentAttributes = this.instrumentAttributeService.getOpenInstrumentAttributesByInstrumentId(instrumentId, tenantId);
        Collection<TransactionActivity> allActivities = new ArrayList<>(0);
        for (Records.ModelRecord model : activeModels) {
                Collection<TransactionActivity> transactionActivities = null;
                try {
                    // Save transaction activities
                    Records.ModelOutputData modelOutputData = this.excelModelExecutor.execute(instrumentId, executionDate,  model,
                            events);
                    this.insertInstrumentAttribute(modelOutputData, DateUtil.convertIntDateToUtc(executionDate),
                            tenantId);
                    List<TransactionActivity> filledTransactions = new ArrayList<>(0);
                    Set<TransactionActivity> transactions = new HashSet<>(0);

                    for (Map<String, Object> activity : modelOutputData.transactionActivityList()) {
                        Object instrumentIdObj = activity.get("instrumentId");
                        if (instrumentIdObj == null || instrumentIdObj.toString().trim().isEmpty()) {
                            log.warn("Skipping activity due to missing or empty instrumentId: {}", activity);
                            continue;
                        }

                        TransactionActivity transactionActivity = this.transactionActivityService.fillTrascationActivity(activity, accountingPeriod);
                        filledTransactions.add(transactionActivity);
                    }



                    for (InstrumentAttribute attr : currentOpenInstrumentAttributes) {
                        List<TransactionActivity> matchingActivities = filledTransactions.stream()
                                .filter(act -> attr.getInstrumentId().equals(act.getInstrumentId())
                                        && attr.getAttributeId().equals(act.getAttributeId()))
                                .toList();
                        List<TransactionActivity> activityList = populateMissingFields(attr, matchingActivities,
                                accountingPeriod, DateUtil.convertIntDateToUtc(executionDate), model.model().getId());
                        transactions.addAll(activityList);
                        // Do something with matchingActivities
                    }

                    // Filter out TransactionActivity where amount is zero
                    Set<TransactionActivity> filteredTransactions = transactions.stream()
                            .filter(transaction -> transaction.getAmount().compareTo(BigDecimal.ZERO) != 0)
                            .collect(Collectors.toSet());
                    // transactionActivities = this.transactionActivityService.save(filteredTransactions);
                    transactionActivities =
                            this.transactionActivityService.getDataService().saveAll(filteredTransactions, tenantId,
                            TransactionActivity.class);
                    allActivities.addAll(transactionActivities);

                } catch (Exception e) {
                    // Log the exception and continue processing the next model
                    log.error("An error occurred while processing model " + model.model().getId() + " for instrument " + instrumentId, e);
                    // Optionally, you can add additional handling here (e.g., notify, retry, etc.)
                    Errors error = Errors.builder().modelId(model.model().getId())
                            .instrumentId(instrumentId)
                            .executionDate(DateUtil.convertIntDateToUtc(executionDate))
                            .code(ErrorCode.Model_Execution_Error)
                            .isWarning(Boolean.FALSE)
                            .stacktrace(StringUtil.getStackTrace(e))
                            .build();
                    this.errorService.save(error);
                    throw e;
                }

        }
return allActivities;
    }

    private void processInstrument(String tenantId, Date executionDate,
                                   AccountingPeriod accountingPeriod
            , String instrumentId
            , List<Records.ModelRecord> activeModels
            , int previousPostingDate
    , int lastActivityPostingDate) throws Throwable {
        log.info("Processing " + instrumentId + " on Thread: " + Thread.currentThread().getName());


         List<InstrumentAttribute> currentOpenInstrumentAttributes = this.instrumentAttributeService.getOpenInstrumentAttributesByInstrumentId(instrumentId, tenantId);
         List<Records.InstrumentAttributeModelRecord> currentOpentInstruments = new ArrayList<>(0);
          for(InstrumentAttribute instrumentAttribute :  currentOpenInstrumentAttributes) {
              Records.InstrumentAttributeModelRecord instrumentAttributeModelRecord = RecordFactory.createInstrumentAttributeModelRecord(InstrumentAttributeVersionType.CURRENT_OPEN_VERSION,instrumentAttribute);
              currentOpentInstruments.add(instrumentAttributeModelRecord);
          }
          // List<InstrumentAttribute> firstVersionOfInstrumentAttributes = this.instrumentAttributeService.getFirstVersionOfInstrumentAttributes(instrumentId, tenantId);

        // Iterate through each model
        for (Records.ModelRecord model : activeModels) {
            Collection<TransactionActivity> transactionActivities = null;
            try {
                // Build Model workflow context
                ModelWorkflowContext context = ModelWorkflowContext.builder()
                        .currentInstrumentAttribute(currentOpentInstruments)
                        .executionType(ModelExecutionType.CHAINED)
                        .accountingPeriod(accountingPeriod)
                        .executionDate(executionDate)
                        .instrumentId(instrumentId)
                        .tenantId(tenantId)
                        .lastActivityPostingDate(lastActivityPostingDate)
                        .excelModel(model).build();

                // Execute the model
                Records.ModelOutputData modelOutputData = this.excelModelExecutor.execute(context);
                    // Save transaction activities

                this.insertInstrumentAttribute(modelOutputData, context.getExecutionDate(), context.getTenantId());
                List<TransactionActivity> filledTransactions = new ArrayList<>(0);
                Set<TransactionActivity> transactions = new HashSet<>(0);

                for (Map<String, Object> activity : modelOutputData.transactionActivityList()) {
                    Object instrumentIdObj = activity.get("instrumentId");
                    if (instrumentIdObj == null || instrumentIdObj.toString().trim().isEmpty()) {
                        log.warn("Skipping activity due to missing or empty instrumentId: {}", activity);
                        continue;
                    }

                    TransactionActivity transactionActivity = this.transactionActivityService.fillTrascationActivity(activity, accountingPeriod);
                    filledTransactions.add(transactionActivity);
                }



                for (InstrumentAttribute attr : currentOpenInstrumentAttributes) {
                    List<TransactionActivity> matchingActivities = filledTransactions.stream()
                            .filter(act -> attr.getInstrumentId().equals(act.getInstrumentId())
                                    && attr.getAttributeId().equals(act.getAttributeId()))
                            .toList();
                    List<TransactionActivity> activityList = populateMissingFields(attr, matchingActivities, accountingPeriod, DateUtil.convertToUtc(context.getExecutionDate()), model.model().getId());
                    transactions.addAll(activityList);
                    // Do something with matchingActivities
                }

                    // Filter out TransactionActivity where amount is zero
                    Set<TransactionActivity> filteredTransactions = transactions.stream()
                            .filter(transaction -> transaction.getAmount().compareTo(BigDecimal.ZERO) != 0)
                            .collect(Collectors.toSet());
                   // transactionActivities = this.transactionActivityService.save(filteredTransactions);
                transactionActivities =  this.transactionActivityService.getDataService().saveAll(filteredTransactions, tenantId, TransactionActivity.class);

                    // Now send message to generate GL
                    // Now send message to Aggregate transactions
                    // this.commonAggregationService.aggregate(transactionActivities, previousPostingDate);
                    LocalDateTime dateTime = LocalDateTime.now();
                    int timestamp = (int) (dateTime.toEpochSecond(ZoneOffset.UTC));
                    String key = String.format("%s-%s-%d", tenantId, "TA", DateUtil.dateInNumber(executionDate));

                 long jobId = System.currentTimeMillis();
                    if (transactionActivities != null) {
                        transactionActivities.forEach(transactionActivity -> {
                                    this.transactionActivityQueue.add(tenantId, jobId, transactionActivity);
                                }
                        );
                    }

                    Records.ExecuteAggregationMessageRecord aggregationMessageRecord = RecordFactory.createExecutionAggregationRecord(tenantId, jobId, (long) DateUtil.dateInNumber(executionDate));
                    aggregationMessageProducer.executeAggregation(aggregationMessageRecord);

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


                Records.GeneralLedgerMessageRecord glRec = RecordFactory.createGeneralLedgerMessageRecord(this.transactionActivityService.getDataService().getTenantId(), jobId);
                generalLedgerMessageProducer.bookTempGL(glRec);

            } catch (Exception e) {
                // Log the exception and continue processing the next model
                log.error("An error occurred while processing model " + model.model().getId() + " for instrument " + instrumentId, e);
                // Optionally, you can add additional handling here (e.g., notify, retry, etc.)
                Errors error = Errors.builder().modelId(model.model().getId())
                        .instrumentId(instrumentId)
                        .executionDate(executionDate)
                        .code(ErrorCode.Model_Execution_Error)
                        .isWarning(Boolean.FALSE)
                        .stacktrace(StringUtil.getStackTrace(e))
                        .build();
                this.errorService.save(error);
                throw e;
            }
        }
    }

    private void insertInstrumentAttribute(Records.ModelOutputData modelOutputData, Date executionDate, String tenantId) throws Exception {
            List<InstrumentAttribute> newInstrumentAttributes = new ArrayList(0);
        for (InstrumentAttribute inputAttr : this.instrumentAttributes(modelOutputData.instrumentAttributeList())) {

            // Fetch currently open instrument attributes
            List<InstrumentAttribute> openAttrs = this.instrumentAttributeService
                    .getOpenInstrumentAttributesByInstrumentId(inputAttr.getInstrumentId(), inputAttr.getAttributeId(), tenantId);

            // Ensure we have at least one open attribute to update
            if (openAttrs != null && !openAttrs.isEmpty()) {
                InstrumentAttribute existingOpenAttr = openAttrs.get(0);

                // Create updated copy of the attribute with new data
                InstrumentAttribute newAttr = copyAndUpdate(
                        existingOpenAttr,
                        executionDate,
                        Source.MODEL,
                        inputAttr.getAttributes()
                );

                // Add both old (closed) and new (updated) to the list
                newInstrumentAttributes.add(existingOpenAttr);
                newInstrumentAttributes.add(newAttr);
                // Mark existing attribute as ended
                existingOpenAttr.setEndDate(newAttr.getEffectiveDate());
            } else {
                log.warn("No open InstrumentAttribute found for instrumentId={} and attributeId={}",
                        inputAttr.getInstrumentId(), inputAttr.getAttributeId());
            }
        }

        for(InstrumentAttribute instrumentAttribute :  newInstrumentAttributes) {
            this.instrumentAttributeService.save(instrumentAttribute);
        }
    }

    public InstrumentAttribute copyAndUpdate(
            InstrumentAttribute original,
            Date executionDate,
            Source newSource,
            Map<String, Object> updatedAttributes) {

        // Deep copy of attributes to avoid mutability issues
        Map<String, Object> copiedAttributes = new HashMap<>();
        if (original.getAttributes() != null) {
            copiedAttributes.putAll(original.getAttributes());
        }


        String value = DateUtil.formatDateToString(executionDate, "MM/dd/yyyy"); // "6/18/2025"; // MM/dd/yyyy
        LocalDate localDate = LocalDate.parse(value, DateTimeFormatter.ofPattern("MM/dd/yyyy"));
        Date postingDate = Date.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());

        // Apply updates
        copiedAttributes.putAll(updatedAttributes);

        // Create a new object (or reuse setters if constructor isn't available)
        InstrumentAttribute copy = new InstrumentAttribute();
        copy.setInstrumentId(original.getInstrumentId());
        copy.setAttributeId(original.getAttributeId());
        copy.setBatchId(original.getBatchId());
        copy.setEndDate(null);
        copy.setPeriodId(original.getPeriodId());
        long versionId = this.instrumentAttributeService.getDataService().generateSequence(SequenceNames.INSTRUMENTATTRIBUTEVERSIONID.name());
        copy.setVersionId(versionId);
        copy.setPreviousVersionId(original.getVersionId());
        copy.setSource(newSource); // updated
        copy.setSourceId(original.getSourceId());
        copy.setAttributes(copiedAttributes); // updated
        copy.setPostingDate(DateUtil.dateInNumber(postingDate)); // updated
        copy.setEffectiveDate(postingDate);
        return copy;
    }

    private List<InstrumentAttribute> instrumentAttributes(List<Map<String, Object>> attributes) throws Exception {
            List<InstrumentAttribute> processInstrumentAttribute = new ArrayList<>(0);

            for(Map<String, Object> attributeMap : attributes) {
                String instrumentId = String.valueOf(attributeMap.get("instrumentId"));
                String attributeId = String.valueOf(attributeMap.get("attributeId"));

                if(instrumentId.isEmpty() || attributeId.isEmpty()) {
                    break;
                }
                    InstrumentAttribute instrumentAttribute = this.processInstrumentAttribute(attributeMap);
                    processInstrumentAttribute.add(instrumentAttribute);
            }
            return processInstrumentAttribute;
    }

    public InstrumentAttribute processInstrumentAttribute(Map<String, Object> item) throws Exception {
        final InstrumentAttribute instrumentAttribute = new InstrumentAttribute();
        final Map<String, Object> attributes = new HashMap<>();
        Date effectiveDate = null;
        String instrumentId = "";
        String attributeId = "";
        int postingDate=0;


        for (Map.Entry<String, Object> entry : item.entrySet()) {
            String key = entry.getKey();
            if(key.isBlank()){
                continue;
            }
            Object value = entry.getValue();
            if(key.equalsIgnoreCase("ACTIVITYUPLOADID")){
                continue;
            } else if (key.equalsIgnoreCase("EFFECTIVEDATE")) {
                continue;
            } else if (key.equalsIgnoreCase("INSTRUMENTID")) {
                instrumentId = String.valueOf(value);
            } else if (key.equalsIgnoreCase("ATTRIBUTEID")) {
                attributeId = String.valueOf(value);
            } else if (key.equalsIgnoreCase("POSTINGDATE")) {
                continue;
            }else {
                if(key.isBlank()){
                    continue;
                }
                attributes.put(key, value);
            }
        }

            return InstrumentAttribute.builder()
                    .postingDate(postingDate)
                    .effectiveDate(effectiveDate)
                    .instrumentId(instrumentId)
                    .attributeId(attributeId)
                    .attributes(attributes)
                    .build();

    }

    private List<TransactionActivity> populateMissingFields(InstrumentAttribute instrumentAttribute,
                                                            List<TransactionActivity> transactionActivities,
                                                            AccountingPeriod accountingPeriod,
                                                            Date executionDate,
                                                            String modelId) throws ParseException {

        Map<String, Object> attributes = this.transactionActivityService.getReclassableAttributes(instrumentAttribute.getAttributes());

        for (TransactionActivity transactionActivity : transactionActivities) {
            transactionActivity.setAccountingPeriod(accountingPeriod);
            transactionActivity.setOriginalPeriodId(accountingPeriod.getPeriodId());
            transactionActivity.setPostingDate(DateUtil.dateInNumber(executionDate));

            if (transactionActivity.getTransactionDate() == null) {
                transactionActivity.setEffectiveDate(DateUtil.dateInNumber(executionDate));
            }

            transactionActivity.setPostingDate(DateUtil.dateInNumber(executionDate));

            if (transactionActivity.getAttributeId() == null || transactionActivity.getAttributeId().isBlank()) {
                transactionActivity.setAttributeId(instrumentAttribute.getAttributeId());
            }

            if (transactionActivity.getInstrumentId() == null || transactionActivity.getInstrumentId().isBlank()) {
                transactionActivity.setInstrumentId(instrumentAttribute.getInstrumentId());
            }

            transactionActivity.setAttributes(attributes);
            transactionActivity.setInstrumentAttributeVersionId(instrumentAttribute.getVersionId());
            transactionActivity.setSource(Source.MODEL);
            transactionActivity.setSourceId(modelId);
        }

        return transactionActivities;
    }
}
