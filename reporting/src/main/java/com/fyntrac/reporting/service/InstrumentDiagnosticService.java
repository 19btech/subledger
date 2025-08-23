package com.fyntrac.reporting.service;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.entity.Model;
import com.fyntrac.common.entity.ModelFile;
import com.fyntrac.common.model.ModelWorkflowContext;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.*;
import com.fyntrac.common.utils.DataUtil;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.ExcelModelUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class InstrumentDiagnosticService {

    private final ExcelModelDiagnosticService excelModelDiagnosticService;
    private final ModelDataService modelDataService;
    private final AccountingPeriodDataUploadService accountingPeriodService;
    private final ExecutionStateService executionStateService;
    private final ModelService modelService;
    private final MemcachedRepository memcachedRepository;
    private final Set<String> keysToExclude;
    private final List<String> instrumentAttributeColumnOrder = List.of(
            "INSTRUMENTID",
            "ATTRIBUTEID",
            "EFFECTIVEDATE",
            "POSTINGDATE",
            "ENDDATE",
            "SOURCE");

    private final List<String> transactionActivityColumnOrder = List.of(
            "INSTRUMENTID",
            "ATTRIBUTEID",
            "TRANSACTIONNAME",
            "AMOUNT",
            "POSTINGDATE",
            "EFFECTIVEDATE"
            );

    private final List<String> balanceColumnOrder = List.of(
            "INSTRUMENTID",
            "ATTRIBUTEID",
            "POSTINGDATE",
            "METRICNAME",
            "BEGINNINGBALANCE",
            "ACTIVITY",
            "ENDINGBALANCE",
            "SERIALVERSIONUID"
    );

    private final List<String> executionStateColumnOrder = List.of("EXECUTIONDATE",
            "LASTEXECUTIONDATE");
    @Autowired
    public InstrumentDiagnosticService(ExcelModelDiagnosticService excelModelDiagnosticService,
                                       ModelDataService modelDataService,
                                       AccountingPeriodDataUploadService accountingPeriodService,
                                       ExecutionStateService executionStateService,
                                       ModelService modelService,
                                       MemcachedRepository memcachedRepository) {
        this.excelModelDiagnosticService = excelModelDiagnosticService;
        this.modelDataService = modelDataService;
        this.accountingPeriodService = accountingPeriodService;
        this.executionStateService = executionStateService;
        this.modelService = modelService;
        this.memcachedRepository = memcachedRepository;
        keysToExclude = Set.of("ATTRIBUTES",
                "ACCOUNTINGPERIOD",
                "REPLAYDATE");

    }

    public Records.DiagnosticReportDataRecord generateDiagnostic(Records.DiagnosticReportRequestRecord requestRecord) throws Throwable {

            ModelWorkflowContext context = this.generateModelOutput(requestRecord);

            List<Map<String, Object>> transactionActivityData = DataUtil.ensureIds(ExcelModelUtil.readSheetAsListOfMaps(context.getWorkbook(),"i_Transaction", "INSTRUMENTID"));
            List<Map<String, Object>> instrumentAttributeData = DataUtil.ensureIds(ExcelModelUtil.readSheetAsListOfMaps(context.getWorkbook(),"i_InstrumentAttribute","INSTRUMENTID"));
            List<Map<String, Object>> balancesData = DataUtil.ensureIds(ExcelModelUtil.readSheetAsListOfMaps(context.getWorkbook(),"i_Metric","POSTINGDATE"));
            List<Map<String, Object>> executionStateData = DataUtil.ensureIds(ExcelModelUtil.readSheetAsListOfMaps(context.getWorkbook(),"i_ExecutionDate",""));

            List<Records.DocumentAttribute> transactionActivityHeader = DataUtil.convert(DataUtil.reorderKeys(transactionActivityData,transactionActivityColumnOrder),keysToExclude);
            List<Records.DocumentAttribute> instrumentAttributeHeader = DataUtil.convert(DataUtil.reorderKeys(instrumentAttributeData,instrumentAttributeColumnOrder),keysToExclude);
            List<Records.DocumentAttribute> balancesHeader = DataUtil.convert(DataUtil.reorderKeys(balancesData,balanceColumnOrder),keysToExclude);
            List<Records.DocumentAttribute> executionStateHeader = DataUtil.convert(DataUtil.reorderKeys(executionStateData,executionStateColumnOrder),keysToExclude);


        // Extract attributeName values in uppercase
            List<String> activityColumnOrderList = transactionActivityHeader.stream()
                .map(Records.DocumentAttribute::attributeName)
                .map(String::toUpperCase)
                .toList();

            List<String> instrumentColumnOrderList = instrumentAttributeHeader.stream()
                .map(Records.DocumentAttribute::attributeName)
                .map(String::toUpperCase)
                .toList();


            List<String> balancesColumnOrderList = executionStateHeader.stream()
                .map(Records.DocumentAttribute::attributeName)
                .map(String::toUpperCase)
                .toList();

            List<String> executionStateColumnOrderList = balancesHeader.stream()
                .map(Records.DocumentAttribute::attributeName)
                .map(String::toUpperCase)
                .toList();
            return RecordFactory.createDiagnosticReportDataRecord(transactionActivityHeader,
                    DataUtil.normalizeIdKeys(DataUtil.reorderKeys(transactionActivityData,activityColumnOrderList),keysToExclude),
                    instrumentAttributeHeader,
                    DataUtil.normalizeIdKeys(DataUtil.reorderKeys(instrumentAttributeData,instrumentColumnOrderList),keysToExclude),
                    balancesHeader,
                    DataUtil.ensureIds(DataUtil.normalizeIdKeys(DataUtil.reorderKeys(balancesData,balancesColumnOrderList),keysToExclude)),
                    executionStateHeader,
                    DataUtil.ensureIds(DataUtil.normalizeIdKeys(DataUtil.reorderKeys(executionStateData,executionStateColumnOrderList),keysToExclude)));
    }

    public ModelWorkflowContext generateModelOutput(Records.DiagnosticReportRequestRecord requestRecord) throws Throwable {
        log.info("Starting diagnostic report generation for tenant={}, instrumentId={}, modelId={}",
                requestRecord.tenant(), requestRecord.instrumentId(), requestRecord.modelId());

        try {
            // 1. Load model
            Model model = Objects.requireNonNull(
                    modelService.getModel(requestRecord.modelId()),
                    "Model not found for ID: " + requestRecord.modelId()
            );

            // 2. Load model file
            ModelFile modelFile = Objects.requireNonNull(
                    modelDataService.getModelFile(model.getModelFileId()),
                    "Model file not found for ID: " + model.getModelFileId()
            );

            Records.ModelRecord modelRecord = RecordFactory.createModelRecord(model, modelFile);

            // 3. Load execution state
            ExecutionState executionState = Objects.requireNonNull(
                    executionStateService.getExecutionState(),
                    "Execution state not found"
            );

            String dateStr = String.valueOf(executionState.getExecutionDate());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

            Date executionDate;
            try {
                executionDate = sdf.parse(dateStr);
            } catch (ParseException e) {
                log.error("Failed to parse execution date={} for tenant={}", dateStr, requestRecord.tenant(), e);
                throw new IllegalArgumentException("Invalid execution date format: " + dateStr, e);
            }

            // 4. Get accounting period
            int accountingPeriodId = com.fyntrac.common.utils.DateUtil.getAccountingPeriodId(executionDate);
            AccountingPeriod accountingPeriod = Objects.requireNonNull(
                    accountingPeriodService.getAccountingPeriod(accountingPeriodId),
                    "AccountingPeriod not found for id=" + accountingPeriodId
            );

            // 5. Generate diagnostic data
            Records.DiagnosticReportModelDataRecord diagnosticReportDataRecord =
                    excelModelDiagnosticService.generateDiagnostic(
                            requestRecord.tenant(),
                            executionDate,
                            executionState.getLastExecutionDate(),
                            accountingPeriod,
                            requestRecord.instrumentId(),
                            modelRecord
                    );

            // 6. Generate output file
            File outputFile = diagnosticReportDataRecord.excelMode();

            if (outputFile == null || !outputFile.exists()) {
                log.error("Failed to generate diagnostic report file for instrumentId={}, tenant={}",
                        requestRecord.instrumentId(), requestRecord.tenant());
                throw new IllegalStateException("Output file not generated: ") ;
            }

            String fileKey = String.format("%s-%s-%d", requestRecord.tenant(), requestRecord.instrumentId(), DateUtil.convertToIntYYYYMMDDFromJavaDate(executionDate));
            this.memcachedRepository.putInCache(fileKey, outputFile);

            log.info("Diagnostic report successfully generated at={} for instrumentId={}, tenant={}",
                    outputFile.getAbsolutePath(), requestRecord.instrumentId(), requestRecord.tenant());

            return diagnosticReportDataRecord.excelData();

        } catch (Exception e) {
            log.error("Error occurred while generating diagnostic report for tenant={}, instrumentId={}",
                    requestRecord.tenant(), requestRecord.instrumentId(), e);
            throw e; // rethrow to bubble up to controller or async handler
        }
    }

    public File getDiagnosticFile(Records.DiagnosticReportRequestRecord requestRecord){

        // 3. Load execution state
        executionStateService.getDataService().setTenantId(requestRecord.tenant());
        ExecutionState executionState = Objects.requireNonNull(
                executionStateService.getExecutionState(),
                "Execution state not found"
        );

        String dateStr = String.valueOf(executionState.getExecutionDate());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date executionDate;
        try {
            executionDate = sdf.parse(dateStr);
        } catch (ParseException e) {
            log.error("Failed to parse execution date={} for tenant={}", dateStr, requestRecord.tenant(), e);
            throw new IllegalArgumentException("Invalid execution date format: " + dateStr, e);
        }

        String fileKey = String.format("%s-%s-%d", requestRecord.tenant(), requestRecord.instrumentId(), DateUtil.convertToIntYYYYMMDDFromJavaDate(executionDate));
        return this.memcachedRepository.getFromCache(fileKey, File.class);
    }
}
