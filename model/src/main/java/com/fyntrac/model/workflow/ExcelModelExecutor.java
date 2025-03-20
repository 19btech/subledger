package com.fyntrac.model.workflow;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.AggregationLevel;
import com.fyntrac.common.service.ExcelFileService;
import com.fyntrac.common.service.InstrumentAttributeService;
import com.fyntrac.common.service.TransactionActivityService;
import com.fyntrac.common.service.aggregation.AttributeLevelAggregationService;
import com.fyntrac.common.service.aggregation.InstrumentLevelAggregationService;
import com.fyntrac.common.service.aggregation.MetricLevelAggregationService;
import com.fyntrac.common.utils.MongoDocumentConverter;
import com.fyntrac.model.utils.ExcelUtil;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class ExcelModelExecutor {

    private final TransactionActivityService transactionService;
    private final InstrumentAttributeService instrumentAttributeService;
    private final InstrumentLevelAggregationService instrumentLevelLtdService;
    private final AttributeLevelAggregationService attributeLevelLtdService;
    private final MetricLevelAggregationService metricLevelLtdService;
    private final ExcelFileService excelFileService;

    @Autowired
    public ExcelModelExecutor(ExcelFileService excelFileService,
                              TransactionActivityService transactionService
                                , InstrumentAttributeService instrumentAttributeService
                                , InstrumentLevelAggregationService instrumentLevelLtdService
                               , AttributeLevelAggregationService attributeLevelLtdService
                               , MetricLevelAggregationService metricLevelLtdService
    ) {
        this.excelFileService = excelFileService;
        this.transactionService = transactionService;
        this.instrumentAttributeService = instrumentAttributeService;
        this.instrumentLevelLtdService = instrumentLevelLtdService;
        this.attributeLevelLtdService = attributeLevelLtdService;
        this.metricLevelLtdService = metricLevelLtdService;
    }

    public Records.ModelOutputData execute(ModelWorkflowContext context) throws Throwable {
        // Load the workbook from the context or a file
        // this.workbook = excelFileService.loadWorkbook(context.getExcelFilePath());

        // Example of how to call loadTransaction
        Workbook workbook = ExcelUtil.convertBinaryToWorkbook(context.excelModel.modelFile().getFileData());

        loadTransactions(context, workbook);
        context.setIInstrumentAttributes(new ArrayList<>(0));
        context.getIInstrumentAttributes().add(MongoDocumentConverter.convertToFlatMap(context.getCurrentInstrumentAttribute()));
        loadFirstInstrumentAttributes(context);
        loadLastInstrumentAttributes(context);
        loadMetrics(context, workbook);

        // Pass the appropriate execution date
        return ExcelModelProcessor.processExcel(context.getCurrentInstrumentAttribute(), context.getExecutionDate(), context.getAccountingPeriod(), workbook, context.getITransactions(), context.getIMetrics(), context.getIInstrumentAttributes() );
    }

    private void  loadTransactions(ModelWorkflowContext context, Workbook workbook) throws Throwable {
        List<String> transactions = this.excelFileService.readExcelSheet(workbook, this.excelFileService.TRANSACTION_SHEET_NAME);
        List<TransactionActivity> transactionActivities =  this.transactionService.fetchTransactions(transactions,context.getInstrumentId(), context.getAttributeId(),  context.getExecutionDate());
        // Process the transactions as needed
        // Create a map for quick lookup
        Map<String, TransactionActivity> activityMap = transactionActivities.stream()
                .collect(Collectors.toMap(TransactionActivity::getTransactionName, activity -> activity));

        // Create a list to maintain the order
        List<Map<String, Object>> orderedTransactionActivities = new ArrayList<>();

        // Iterate through the transactions list and add corresponding activities in order
        for (String transactionName : transactions) {
            TransactionActivity activity = activityMap.get(transactionName);
            if (activity != null) {
                orderedTransactionActivities.add(MongoDocumentConverter.convertToMap(activity));
            }
        }
        context.setITransactions(orderedTransactionActivities);
    }

    private void  loadLastInstrumentAttributes(ModelWorkflowContext context) throws Throwable {

        long lastInstrumentAttributeVersionId = context.getCurrentInstrumentAttribute().getPreviousVersionId();
       InstrumentAttribute instrumentAttribute = this.instrumentAttributeService.getInstrumentAttributeByVersionId(lastInstrumentAttributeVersionId);
        if(instrumentAttribute == null) {
            return;
        }
        context.setLastInstrumentAttribute(instrumentAttribute);
        context.getIInstrumentAttributes().add(MongoDocumentConverter.convertToFlatMap(instrumentAttribute));
    }

    private void  loadFirstInstrumentAttributes(ModelWorkflowContext context) throws Throwable {
        InstrumentAttribute instrumentAttribute = this.instrumentAttributeService.getFirstVersionOfInstrumentAttributes(context.getInstrumentId(), context.getAttributeId());
        if(instrumentAttribute == null) {
            return;
        }
        context.setFirstInstrumentAttribute(instrumentAttribute);
        context.getIInstrumentAttributes().add(MongoDocumentConverter.convertToFlatMap(instrumentAttribute));
    }

    private void  loadMetrics(ModelWorkflowContext context, Workbook workbook) throws Throwable {
        List<String> metrics = this.excelFileService.readExcelSheet(workbook, this.excelFileService.METRIC_SHEET_NAME);
        Map<String, Records.MetricRecord> metricsMap = new HashMap<>(0);
        Model model = context.excelModel.model();
        AggregationLevel aggregationLevel = model.getModelConfig().getAggregationLevel();
        switch (aggregationLevel){
            case AggregationLevel.ATTRIBUTE -> {
                    List<AttributeLevelLtd> ltds = this.attributeLevelLtdService.getBalance(context.getInstrumentId(), context.getAttributeId(), metrics, context.getAccountingPeriod().getPeriodId());
                    for(AttributeLevelLtd ltd : ltds) {
                        metricsMap.put(ltd.getMetricName(), RecordFactory.createMetricRecord(ltd));
                    }
            }
            case AggregationLevel.INSTRUMENT -> {
                List<InstrumentLevelLtd> ltds = this.instrumentLevelLtdService.getBalance(context.getInstrumentId(), metrics, context.getAccountingPeriod().getPeriodId());
                for(InstrumentLevelLtd ltd : ltds) {
                    metricsMap.put(ltd.getMetricName(), RecordFactory.createMetricRecord(ltd));
                }
            }
            case AggregationLevel.TENANT ->  {
                List<MetricLevelLtd> ltds = this.metricLevelLtdService.getBalance( metrics, context.getAccountingPeriod().getPeriodId());
                for(MetricLevelLtd ltd : ltds) {
                    metricsMap.put(ltd.getMetricName(), RecordFactory.createMetricRecord(ltd));
                }
            }
            default -> {}
        };

        // Create a list to maintain the order
        List<Map<String, Object>> orderedMetrics = new ArrayList<>();

        // Iterate through the transactions list and add corresponding activities in order
        for (String metricName : metrics) {
            Records.MetricRecord metricRecord = metricsMap.get(metricName.toUpperCase());
            if (metricRecord != null) {
                orderedMetrics.add(MongoDocumentConverter.convertToMap(metricRecord));
            }
        }

        context.setIMetrics(orderedMetrics);
    }

    private void execute(ModelWorkflowContext context, Workbook workbook) throws IOException {
        ExcelModelProcessor.processExcel(context.getCurrentInstrumentAttribute(), context.getExecutionDate(), context.getAccountingPeriod(), workbook, context.getITransactions(), context.getIMetrics(), context.getIInstrumentAttributes());
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