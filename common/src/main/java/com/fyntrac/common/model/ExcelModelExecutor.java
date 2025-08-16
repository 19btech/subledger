package com.fyntrac.common.model;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.AggregationLevel;
import com.fyntrac.common.enums.InstrumentAttributeVersionType;
import com.fyntrac.common.exception.InstrumentAttributeVersionTypeException;
import com.fyntrac.common.service.*;
import com.fyntrac.common.service.aggregation.AttributeLevelAggregationService;
import com.fyntrac.common.service.aggregation.InstrumentLevelAggregationService;
import com.fyntrac.common.service.aggregation.MetricLevelAggregationService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.MongoDocumentConverter;
import com.fyntrac.common.utils.ExcelModelUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ExcelModelExecutor {

    private final TransactionActivityService transactionService;
    private final InstrumentAttributeService instrumentAttributeService;
    private final InstrumentLevelAggregationService instrumentLevelLtdService;
    private final AttributeLevelAggregationService attributeLevelLtdService;
    private final MetricLevelAggregationService metricLevelLtdService;
    private final ExcelFileService excelFileService;
    private final ExecutionStateService executionStateService;
    private final InstrumentReplayStateService instrumentReplayStateService;
    @Value("${fyntrac.model.generate.model.output.file}")
    boolean generateModelOutputFile;

    @Autowired
    public ExcelModelExecutor(ExcelFileService excelFileService,
                              TransactionActivityService transactionService
            , InstrumentAttributeService instrumentAttributeService
            , InstrumentLevelAggregationService instrumentLevelLtdService
            , AttributeLevelAggregationService attributeLevelLtdService
            , MetricLevelAggregationService metricLevelLtdService
            , ExecutionStateService executionStateService
            , InstrumentReplayStateService instrumentReplayStateService
    ) {
        this.excelFileService = excelFileService;
        this.transactionService = transactionService;
        this.instrumentAttributeService = instrumentAttributeService;
        this.instrumentLevelLtdService = instrumentLevelLtdService;
        this.attributeLevelLtdService = attributeLevelLtdService;
        this.metricLevelLtdService = metricLevelLtdService;
        this.executionStateService = executionStateService;
        this.instrumentReplayStateService = instrumentReplayStateService;
    }

    public Records.ModelOutputData execute(ModelWorkflowContext context) throws Throwable {
        // Load the workbook from the context or a file
        // this.workbook = excelFileService.loadWorkbook(context.getExcelFilePath());

        // Example of how to call loadTransaction
        Workbook workbook = ExcelModelUtil.convertBinaryToWorkbook(context.excelModel.modelFile().getFileData());

        ExecutionState executionState = executionStateService.getExecutionState();
        if(executionState == null) {
            executionState = ExecutionState.builder()
                    .executionDate(0)
                    .lastExecutionDate(0)
                    .build();
        }

        context.setExecutionState(executionState);
        context.setIInstrumentAttributes(new ArrayList<>(0));
        loadTransactions(context, workbook);
        loadExecutionDate(context, workbook);


        List<String> instrumentAttributeVersionTypes = this.excelFileService.readExcelSheet(workbook, this.excelFileService.INSTRUMENT_ATTRIBUTE_SHEET_NAME);

        for(String versionType : instrumentAttributeVersionTypes) {
            try {
                InstrumentAttributeVersionType iavt = InstrumentAttributeVersionType.valueOf(versionType.toUpperCase());
                if(iavt.equals(InstrumentAttributeVersionType.FIRST_VERSION)) {
                    loadFirstInstrumentAttributes(context);
                }else if(iavt.equals(InstrumentAttributeVersionType.LAST_CLOSED_VERSION)) {
                    loadLastInstrumentAttributes(context);
                }
                log.info("InstrumentAttribute verion type: " + iavt);
            } catch (IllegalArgumentException e) {
                log.error("Invalid enum value: " + versionType);
                throw new InstrumentAttributeVersionTypeException("InstrumentAttribute verion type['" + versionType + "' not a valid version type in sheet ["+ this.excelFileService.INSTRUMENT_ATTRIBUTE_SHEET_NAME +"], please correct model first then upload again");
            }

        }

        addInstrumentAttribute(context);
        loadMetrics(context, workbook);
        // Pass the appropriate execution date
        Records.ModelOutputData outputData = ExcelModelProcessor.processExcel(context.getInstrumentId(), context.getCurrentInstrumentAttribute(), context.getExecutionDate(), context.getAccountingPeriod(), workbook, context.getITransactions(), context.getIMetrics(), context.getIInstrumentAttributes(), context.getIExecutionDate(), generateModelOutputFile );
        workbook.close();
        return  outputData;
    }

    public Workbook generateDiagnostic(ModelWorkflowContext context) throws Throwable {
        // Load the workbook from the context or a file
        // this.workbook = excelFileService.loadWorkbook(context.getExcelFilePath());

        // Example of how to call loadTransaction
        Workbook workbook = ExcelModelUtil.convertBinaryToWorkbook(context.excelModel.modelFile().getFileData());

        ExecutionState executionState = executionStateService.getExecutionState();
        if(executionState == null) {
            executionState = ExecutionState.builder()
                    .executionDate(0)
                    .lastExecutionDate(0)
                    .build();
        }

        context.setExecutionState(executionState);
        context.setIInstrumentAttributes(new ArrayList<>(0));
        loadTransactions(context, workbook);
        loadExecutionDate(context, workbook);


        List<String> instrumentAttributeVersionTypes = this.excelFileService.readExcelSheet(workbook, this.excelFileService.INSTRUMENT_ATTRIBUTE_SHEET_NAME);

        for(String versionType : instrumentAttributeVersionTypes) {
            try {
                InstrumentAttributeVersionType iavt = InstrumentAttributeVersionType.valueOf(versionType.toUpperCase());
                if(iavt.equals(InstrumentAttributeVersionType.FIRST_VERSION)) {
                    loadFirstInstrumentAttributes(context);
                }else if(iavt.equals(InstrumentAttributeVersionType.LAST_CLOSED_VERSION)) {
                    loadLastInstrumentAttributes(context);
                }
                log.info("InstrumentAttribute verion type: " + iavt);
            } catch (IllegalArgumentException e) {
                log.error("Invalid enum value: " + versionType);
                throw new InstrumentAttributeVersionTypeException("InstrumentAttribute verion type['" + versionType + "' not a valid version type in sheet ["+ this.excelFileService.INSTRUMENT_ATTRIBUTE_SHEET_NAME +"], please correct model first then upload again");
            }

        }

        addInstrumentAttribute(context);
        loadMetrics(context, workbook);
        // Pass the appropriate execution date
        return ExcelModelProcessor.generateOutput(context.getInstrumentId(), context.getCurrentInstrumentAttribute(), context.getExecutionDate(), context.getAccountingPeriod(), workbook, context.getITransactions(), context.getIMetrics(), context.getIInstrumentAttributes(), context.getIExecutionDate());
    }

    private void addInstrumentAttribute(ModelWorkflowContext context) {
        for(Records.InstrumentAttributeModelRecord instrumentAttribute : context.getCurrentInstrumentAttribute()) {
            context.getIInstrumentAttributes().add(MongoDocumentConverter.convertRecordToFlatMap(instrumentAttribute));
        }

    }
    private List<TransactionActivity> getActivities(ModelWorkflowContext context, List<String> upperCaseTransactions) {
        List<TransactionActivity> activityList = new ArrayList<>(0);
        for(Records.InstrumentAttributeModelRecord instrumentAttribute : context.getCurrentInstrumentAttribute()) {
            List<TransactionActivity> transactionActivities =  this.transactionService.fetchTransactions(upperCaseTransactions,instrumentAttribute.instrumentId(), instrumentAttribute.attributeId(),  context.getExecutionDate());
            activityList.addAll(transactionActivities);
        }
        return  activityList;
    }
    private void  loadTransactions(ModelWorkflowContext context, Workbook workbook) throws Throwable {
        List<String> transactionList = this.excelFileService.readExcelSheet(workbook, this.excelFileService.TRANSACTION_SHEET_NAME);

        List<String> uniqueTransactionList = transactionList.stream()
                .distinct() // Remove duplicates
                .collect(Collectors.toList());

        List<String> upperCaseTransactions = uniqueTransactionList.stream()
                .map(String::toUpperCase) // Convert each string to uppercase
                .collect(Collectors.toList()); // Collect the results into a new List

        List<TransactionActivity> transactionActivities =  getActivities(context, upperCaseTransactions);

        // Group by UPPERCASE transactionName & sort each list by effectiveDate (DESC)
        Map<String, List<TransactionActivity>> transactionsMapByName = transactionActivities.stream()
                .collect(Collectors.groupingBy(
                        transaction -> transaction.getTransactionName().toUpperCase(), // Convert key to uppercase
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                list -> {
                                    list.sort(Comparator.comparingInt(TransactionActivity::getEffectiveDate).reversed());
                                    return list;
                                }
                        )
                ));

        // Create a list to maintain the order
        List<Map<String, Object>> orderedTransactionActivities = new ArrayList<>();

        // Iterate through the transactions list and add corresponding activities in order
        for (String transactionName : upperCaseTransactions) {
            List<TransactionActivity> activityList = transactionsMapByName.get(transactionName);
            if (activityList != null) {
                for(TransactionActivity activity :  activityList) {
                    Records.TransactionActivityModelRecord modelRecord =  RecordFactory.createTransactionActivityModelRecord(activity);
                    orderedTransactionActivities.add(MongoDocumentConverter.convertToMap(modelRecord));
                }
            }
        }
        context.setITransactions(orderedTransactionActivities);
    }

    private void  loadExecutionDate(ModelWorkflowContext context, Workbook workbook) throws Throwable {
        List<String> executionDates = this.excelFileService.readExcelSheet(workbook, this.excelFileService.EXECUTION_DATE_SHEET_NAME);

        // Create a list to maintain the order
        List<Map<String, Object>> iExecutionDate = new ArrayList<>();
        ExecutionState executionState = this.executionStateService.getExecutionState();
        // Iterate through the transactions list and add corresponding activities in order
        Date lastExecutionDate = null;
        Date replayDate = null;
        int intExecutionDate = DateUtil.dateInNumber(context.getExecutionDate());
        if (executionState != null) {
            if (executionState.getExecutionDate() != null && intExecutionDate > executionState.getExecutionDate()) {
                lastExecutionDate = DateUtil.convertToDateFromYYYYMMDD(executionState.getExecutionDate());
            }else if(executionState.getLastExecutionDate() != 0) {
                lastExecutionDate = DateUtil.convertToDateFromYYYYMMDD(executionState.getLastExecutionDate());
            }

            InstrumentReplayState instrumentReplayState =  this.instrumentReplayStateService.getInstrumentReplayState(context.getInstrumentId(), intExecutionDate);
            if(instrumentReplayState != null) {
                replayDate = DateUtil.convertToDateFromYYYYMMDD(instrumentReplayState.getMinEffectiveDate());
            }
            Records.ExecutionDateRecord executionDateRecord = RecordFactory.createExcutionDateRecord(context.getExecutionDate(), lastExecutionDate, replayDate);
            iExecutionDate.add(MongoDocumentConverter.convertToMap(executionDateRecord));
            context.setIExecutionDate(iExecutionDate);
        }
    }

    private void  loadLastInstrumentAttributes(ModelWorkflowContext context) throws Throwable {

        for(Records.InstrumentAttributeModelRecord instrumentAttributeModelRecord :  context.currentInstrumentAttribute) {
            long lastInstrumentAttributeVersionId = instrumentAttributeModelRecord.previousVersionId();
            InstrumentAttribute instrumentAttribute = this.instrumentAttributeService.getInstrumentAttributeByVersionId(lastInstrumentAttributeVersionId);
            if(instrumentAttribute == null) {
                continue;
            }
            // context.setLastInstrumentAttribute(instrumentAttribute);
            Records.InstrumentAttributeModelRecord previousInstrumentAttributeModelRecord = RecordFactory.createInstrumentAttributeModelRecord(InstrumentAttributeVersionType.LAST_CLOSED_VERSION,instrumentAttribute);

            context.getIInstrumentAttributes().add(MongoDocumentConverter.convertRecordToFlatMap(previousInstrumentAttributeModelRecord));
        }

    }

    private void  loadFirstInstrumentAttributes(ModelWorkflowContext context) throws Throwable {
        for(Records.InstrumentAttributeModelRecord instrumentAttributeModelRecord :  context.currentInstrumentAttribute) {
            InstrumentAttribute instrumentAttribute = this.instrumentAttributeService.getFirstVersionOfInstrumentAttributes(instrumentAttributeModelRecord.instrumentId(), instrumentAttributeModelRecord.attributeId());
            if(instrumentAttribute == null) {
                continue;
            }
            // context.setFirstInstrumentAttribute(instrumentAttribute);
            Records.InstrumentAttributeModelRecord fvInstrumentAttributeModelRecord = RecordFactory.createInstrumentAttributeModelRecord(InstrumentAttributeVersionType.FIRST_VERSION,instrumentAttribute);

            context.getIInstrumentAttributes().add(MongoDocumentConverter.convertRecordToFlatMap(fvInstrumentAttributeModelRecord));
        }

    }

    private List<AttributeLevelLtd> getAttributeLevelLtdBalance(ModelWorkflowContext context, List<String> metrics, int executionDate) {
        List<AttributeLevelLtd> balances = new ArrayList<>(0);
        for(Records.InstrumentAttributeModelRecord instrumentAttribute : context.getCurrentInstrumentAttribute()) {
            List<AttributeLevelLtd> balancesList = this.attributeLevelLtdService.getBalance(instrumentAttribute.instrumentId(), instrumentAttribute.attributeId(), metrics, executionDate);
            balances.addAll(balancesList);
        }
        return balances;
    }


    private List<InstrumentLevelLtd> getInstrumentIdLevelLtdBalance(ModelWorkflowContext context, List<String> metrics, int executionDate) {
        List<InstrumentLevelLtd> balances = new ArrayList<>(0);
        for(Records.InstrumentAttributeModelRecord instrumentAttribute : context.getCurrentInstrumentAttribute()) {
            List<InstrumentLevelLtd> balancesList = this.instrumentLevelLtdService.getBalance(instrumentAttribute.instrumentId(), metrics, executionDate);
            balances.addAll(balancesList);
        }
        return balances;
    }

    private List<MetricLevelLtd> getMetricLevelLtdBalance(ModelWorkflowContext context, List<String> metrics, int executionDate) {
        List<MetricLevelLtd> balances = new ArrayList<>(0);
        for(Records.InstrumentAttributeModelRecord instrumentAttribute : context.getCurrentInstrumentAttribute()) {
            List<MetricLevelLtd> balancesList = this.metricLevelLtdService.getBalance( metrics, executionDate);
            balances.addAll(balancesList);
        }
        return balances;
    }

    private void  loadMetrics(ModelWorkflowContext context, Workbook workbook) throws Throwable {
        List<String> metrics = this.excelFileService.readExcelSheet(workbook, this.excelFileService.METRIC_SHEET_NAME);
        List<Records.MetricRecord> balances = new ArrayList<>(0);
        Model model = context.excelModel.model();
        AggregationLevel aggregationLevel = model.getModelConfig().getAggregationLevel();
        int executionDate =  context.getLastActivityPostingDate();

        if(context.getExecutionState().getExecutionDate() >  executionDate) {
            executionDate = context.getExecutionState().getExecutionDate();
        }
        switch (aggregationLevel){
            case AggregationLevel.ATTRIBUTE -> {
                List<AttributeLevelLtd> ltds = getAttributeLevelLtdBalance(context, metrics, executionDate);
                for(AttributeLevelLtd ltd : ltds) {
                    balances.add(RecordFactory.createMetricRecord(ltd));
                }
            }
            case AggregationLevel.INSTRUMENT -> {
                List<InstrumentLevelLtd> ltds = this.getInstrumentIdLevelLtdBalance(context, metrics, executionDate);
                for(InstrumentLevelLtd ltd : ltds) {
                    balances.add(RecordFactory.createMetricRecord(ltd));
                }
            }
            case AggregationLevel.TENANT ->  {
                List<MetricLevelLtd> ltds = this.getMetricLevelLtdBalance( context, metrics, executionDate);
                for(MetricLevelLtd ltd : ltds) {
                    balances.add(RecordFactory.createMetricRecord(ltd));
                }
            }
            default -> {}
        };

        // Create a list to maintain the order
        List<Map<String, Object>> orderedMetrics = new ArrayList<>();

        // Iterate through the transactions list and add corresponding activities in order
        for (Records.MetricRecord record : balances) {
            if (record != null) {
                orderedMetrics.add(MongoDocumentConverter.convertToMap(record));
            }
        }

        context.setIMetrics(orderedMetrics);
    }

    private void execute(ModelWorkflowContext context, Workbook workbook) throws IOException {
        ExcelModelProcessor.processExcel(context.getInstrumentId(), context.getCurrentInstrumentAttribute(), context.getExecutionDate(), context.getAccountingPeriod(), workbook, context.getITransactions(), context.getIMetrics(), context.getIInstrumentAttributes(), context.getIExecutionDate(), generateModelOutputFile);
    }

    private void valuateModel() {

    }

    private void getOutput() {

    }

    private void getTransactionsOutput() {

    }

    private void getInstrumentAttributesOutput() {

    }
}