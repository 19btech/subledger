package com.fyntrac.common.service;

import com.fyntrac.common.component.TransactionActivityQueue;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.InstrumentAttributeVersionType;
import com.fyntrac.common.enums.SequenceNames;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.exception.LoadExcelModelExecption;
import com.fyntrac.common.model.ExcelModelExecutor;
import com.fyntrac.common.model.ModelWorkflowContext;
import com.fyntrac.common.repository.EventRepository;
import com.fyntrac.common.repository.InstrumentAttributeRepository;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.aggregation.CommonAggregationService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.ExcelModelUtil;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
@Slf4j
public class ExcelModelDiagnosticService {

    private final EventRepository eventRepository;

    private final InstrumentAttributeRepository instrumentRepo;
    private final ModelDataService modelDataService;
    private final MemcachedRepository memcachedRepository;
    private final AccountingPeriodDataUploadService accountingPeriodService;
    private final ExcelModelExecutor excelModelExecutor;
    private final TransactionActivityService transactionActivityService;
    private final ErrorService errorService;
    private final CommonAggregationService commonAggregationService;
    private final ExecutionStateService executionStateService;
    private final InstrumentAttributeService instrumentAttributeService;
    private final TransactionActivityQueue transactionActivityQueue;
    public ExcelModelDiagnosticService(ModelDataService modelDataService
            , MemcachedRepository memcachedRepository
            , AccountingPeriodDataUploadService accountingPeriodService
            , ExcelModelExecutor excelModelExecutor
            , TransactionActivityService transactionActivityService
            , ErrorService errorService
            , CommonAggregationService commonAggregationService
            , ExecutionStateService executionStateService
            , InstrumentAttributeService instrumentAttributeService
            , TransactionActivityQueue transactionActivityQueue
                                       , EventRepository eventRepository
    , InstrumentAttributeRepository instrumentRepo) {
        this.modelDataService = modelDataService;
        this.memcachedRepository = memcachedRepository;
        this.accountingPeriodService = accountingPeriodService;
        this.excelModelExecutor = excelModelExecutor;
        this.transactionActivityService = transactionActivityService;
        this.errorService = errorService;
        this.commonAggregationService = commonAggregationService;
        this.executionStateService = executionStateService;
        this.instrumentAttributeService = instrumentAttributeService;
        this.transactionActivityQueue = transactionActivityQueue;
        this.instrumentRepo = instrumentRepo;
        this.eventRepository = eventRepository;

    }


    public Records.DiagnosticReportModelDataRecord generateEventDiagnostic(Records.DiagnosticReportRequestRecord requestRecord, Records.ModelRecord modelRecord) throws IOException, ParseException {
        final String tenant = TenantContextHolder.getTenant();

        int postingDate = Integer.parseInt(requestRecord.postingDate());
        String instrumentId = requestRecord.instrumentId();
        List<Event> events = this.eventRepository.findByPostingDateAndInstrumentId(postingDate, instrumentId);

        Workbook workbook = this.excelModelExecutor.executeExcelModel(
        modelRecord,
        events);

        File modelFile = generateModelOutputFile(tenant, instrumentId, workbook);
        // Close the workbook explicitly
        workbook.close();

        // Build workflow context
        ModelWorkflowContext context = ModelWorkflowContext.builder()
                .currentInstrumentAttribute(null)
                .executionType(com.fyntrac.common.enums.ModelExecutionType.CHAINED)
                .accountingPeriod(null)
                .executionDate(DateUtil.convertIntDateToUtc(postingDate))
                .instrumentId(instrumentId)
                .tenantId(tenant)
                .lastActivityPostingDate(postingDate)
                .events(events)
                .excelModel(modelRecord)
                .build();
        context.setWorkbook(workbook);
        // Return the saved file
        return RecordFactory.createDiagnosticReportModelDataRecord(context, modelFile);

    }

    public Set<TransactionActivity> getTransactionActivities(
            List<InstrumentAttribute> currentOpenInstrumentAttributes
            , Records.ModelOutputData modelOutputData
            , AccountingPeriod accountingPeriod
            , Date executionDate
            , Records.ModelRecord model
    ) {
        Set<TransactionActivity> transactionActivities = new HashSet<>(0);
        for(InstrumentAttribute instrumentAttribute : currentOpenInstrumentAttributes) {

            Set<TransactionActivity> transactions = transactionActivityService.generateTransactions(
                    modelOutputData.transactionActivityList(),
                    instrumentAttribute,
                    accountingPeriod,
                    executionDate,
                    Source.MODEL,
                    model.model().getId()
            );
            transactionActivities.addAll(transactions);
        }
        return  transactionActivities;
    }

    public Records.DiagnosticReportModelDataRecord generateDiagnostic(String tenantId,
                                                                      Date executionDate,
                                                                      int lastActivityPostingDate,
                                                                      AccountingPeriod accountingPeriod,
                                                                      String instrumentId,
                                                                      Records.ModelRecord model) throws Throwable {

        // Fetch instrument attributes
        Integer postingDate = DateUtil.convertToIntYYYYMMDDFromJavaDate(executionDate);
        String modelId = model.model().getId();
        List<InstrumentAttribute> currentOpenInstrumentAttributes =
                this.instrumentAttributeService.getOpenInstrumentAttributesByInstrumentId(instrumentId,modelId, postingDate, tenantId);

        List<Records.InstrumentAttributeModelRecord> currentOpenInstruments = new ArrayList<>();
        for (InstrumentAttribute instrumentAttribute : currentOpenInstrumentAttributes) {
            Records.InstrumentAttributeModelRecord record =
                    RecordFactory.createInstrumentAttributeModelRecord(
                            InstrumentAttributeVersionType.CURRENT_OPEN_VERSION,
                            instrumentAttribute
                    );
            currentOpenInstruments.add(record);
        }

        // Build workflow context
        ModelWorkflowContext context = ModelWorkflowContext.builder()
                .currentInstrumentAttribute(currentOpenInstruments)
                .executionType(com.fyntrac.common.enums.ModelExecutionType.CHAINED)
                .accountingPeriod(accountingPeriod)
                .executionDate(executionDate)
                .instrumentId(instrumentId)
                .tenantId(tenantId)
                .lastActivityPostingDate(lastActivityPostingDate)
                .excelModel(model)
                .build();

        // Generate workbook
        Workbook workbook = this.excelModelExecutor.generateDiagnostic(context);



        File modelFile = generateModelOutputFile(tenantId, instrumentId, workbook);
        // Close the workbook explicitly
        workbook.close();

        context.setWorkbook(workbook);
        // Return the saved file
        return RecordFactory.createDiagnosticReportModelDataRecord(context, modelFile);
    }

    public File generateModelOutputFile(String tenant,String instrumentId,  Workbook workbook) throws IOException {

        // Create output file
        String fileName = String.format("processed_output_%s_%s.xlsx",tenant, instrumentId);
        File outputFile = new File(fileName);

        // Save the file
        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            workbook.write(fos);
        }
        return outputFile;
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

    public List<InstrumentAttribute> instrumentAttributes(List<Map<String, Object>> attributes) throws Exception {
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

    public List<TransactionActivity> populateMissingFields(InstrumentAttribute instrumentAttribute,
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


    public Map<Integer, Records.ExcelModelRecord> loadModels(List<Records.ModelRecord> activeModels) throws Exception {
        Map<Integer, Records.ExcelModelRecord> excelModelsMap = new HashMap<>();
        int priority = 1;

        for (Records.ModelRecord modelRecord : activeModels) {
            try {
                Records.ExcelModelRecord excelModel = RecordFactory.createExcelModelRecord(
                        modelRecord.model(),
                        ExcelModelUtil.convertBinaryToWorkbook(modelRecord.modelFile().getFileData())
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
