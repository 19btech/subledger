package com.fyntrac.reporting.service;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.Event;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.entity.Model;
import com.fyntrac.common.entity.ModelFile;
import com.fyntrac.common.model.ModelWorkflowContext;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.*;
import com.fyntrac.common.utils.DataUtil;
import com.fyntrac.common.utils.ExcelModelUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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

        Map<String, List<Map<String, Object>>> valueMap = this.getEventValueMap(context.getEvents());

        return RecordFactory.createDiagnosticReportDataRecord(valueMap);
    }

    public List<Map<String, Map<String, Object>>> convert(Map<String, Map<String, Object>> input) {
        return input.entrySet()
                .stream()
                .map(e -> Map.of(e.getKey(), e.getValue()))
                .toList();
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

            String dateStr = requestRecord.postingDate();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));




            // 5. Generate diagnostic data
            Records.DiagnosticReportModelDataRecord diagnosticReportDataRecord =
                    excelModelDiagnosticService.generateEventDiagnostic(requestRecord,
                            modelRecord);


            // 6. Generate output file
            File outputFile = diagnosticReportDataRecord.excelMode();

            if (outputFile == null || !outputFile.exists()) {
                log.error("Failed to generate diagnostic report file for instrumentId={}, tenant={}",
                        requestRecord.instrumentId(), requestRecord.tenant());
                throw new IllegalStateException("Output file not generated: ") ;
            }

            String fileKey = String.format("%s-%s-%s", requestRecord.tenant(), requestRecord.instrumentId(),
                    requestRecord.postingDate());
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

        String dateStr = requestRecord.postingDate();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date executionDate;
        try {
            executionDate = sdf.parse(dateStr);
        } catch (ParseException e) {
            log.error("Failed to parse execution date={} for tenant={}", dateStr, requestRecord.tenant(), e);
            throw new IllegalArgumentException("Invalid execution date format: " + dateStr, e);
        }

        String fileKey = String.format("%s-%s-%s", requestRecord.tenant(), requestRecord.instrumentId(),
                dateStr);
        return this.memcachedRepository.getFromCache(fileKey, File.class);
    }

    Map<String, List<Map<String, Object>>> getEventValueMap(List<Event> events) {

        Map<String, List<Map<String, Object>>> valueMap = new HashMap<>(0);

        for(Event event : events) {
            Map<String,Map<String, Object>> values = event.getEventDetail().getValues();
            List<Map<String, Object>> tmpValueMap = new ArrayList<>(0);
            for(Map<String, Object> map : values.values()) {
                tmpValueMap.add(map);
            }

            valueMap.put(event.getEventName(), tmpValueMap);
        }

        return valueMap;
    }
}
