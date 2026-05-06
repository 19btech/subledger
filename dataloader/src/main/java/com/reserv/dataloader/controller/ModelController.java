package com.reserv.dataloader.controller;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.Model;
import com.fyntrac.common.entity.ModelConfig;
import com.fyntrac.common.entity.ModelExecutionBatchLog;
import com.fyntrac.common.enums.ModelStatus;
import com.fyntrac.common.enums.ModelType;
import com.fyntrac.common.service.ExcelModelService;
import com.fyntrac.common.service.ModelService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.StringUtil;
import com.reserv.dataloader.service.DataloaderExcelFileService;
import com.reserv.dataloader.service.ModelUploadService;
import com.reserv.dataloader.service.model.EventConfigurationValidator;
import com.reserv.dataloader.service.model.ModelExecutionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/dataloader/model")
@Slf4j
public class ModelController {

    private final DataloaderExcelFileService fileService;
    private final ModelService modelService;
    private final ModelUploadService modelUploadService;
    private final ModelExecutionService modelExecutionService;
    private final ExcelModelService excelModelService;
    private final com.fyntrac.common.service.ExecutionStateService executionStateService;
    private final com.fyntrac.common.repository.ModelExecutionBatchLogRepository modelExecutionBatchLogRepository;

    @Autowired
    public ModelController(DataloaderExcelFileService fileService
            , ModelService modelServicen
            , ModelUploadService modelUploadService
            , ModelExecutionService modelExecutionService
            , ExcelModelService excelModelService
            , com.fyntrac.common.service.ExecutionStateService executionStateService
            , com.fyntrac.common.repository.ModelExecutionBatchLogRepository modelExecutionBatchLogRepository) {
        this.fileService = fileService;
        this.modelService = modelServicen;
        this.modelUploadService = modelUploadService;
        this.modelExecutionService = modelExecutionService;
        this.excelModelService = excelModelService;
        this.executionStateService = executionStateService;
        this.modelExecutionBatchLogRepository = modelExecutionBatchLogRepository;
    }

    // Upload endpoint
    @PostMapping("/upload")
    @Transactional
    public ResponseEntity<?> uploadFile(@RequestParam("files") MultipartFile file,
                                        @RequestParam("modelName") String modelName,
                                        @RequestParam("modelOrderId") String modelOrderId) {
        try {

            // Basic null/empty file safety check
            if (file == null || file.isEmpty()) {
                return ResponseEntity.badRequest().body("Uploaded file is empty.");
            }

            // Check if model already exists
            Model model = modelService.getModelByName(modelName);
            if (model != null) {
                model.setIsDeleted(1);
                modelService.save(model);
            }

            // Validate the uploaded model file
            EventConfigurationValidator.ValidationResult result =
                    modelUploadService.validateNewModel(file);

            if (result == null || !result.isValid()) {
                String errorMessage = String.format(
                        "Model [%s] validation failed. Errors: %s",
                        modelName,
                        result != null ? result.getErrors() : "Unknown validation error"
                );

                log.warn(errorMessage);
                return ResponseEntity.badRequest().body(errorMessage);
            }

            // Upload file and save model
            String fileId = fileService.uploadFile(file);

            ModelConfig modelConfig = new ModelConfig();

            model = modelService.save(
                    modelName,
                    ModelType.EXCEL,
                    modelOrderId,
                    fileId,
                    Boolean.FALSE,
                    ModelStatus.INACTIVE,
                    new Date(),
                    "Fyntrac",
                    modelConfig
            );

            return ResponseEntity.ok(model);

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());

        } catch (Exception e) {
            String stackTrace = com.fyntrac.common.utils.StringUtil.getStackTrace(e);
            log.error(stackTrace);
            return ResponseEntity.internalServerError()
                    .body("An unexpected error occurred: " + e.getMessage());
        }
    }


    @PostMapping("/upload-dsl-model")
    @Transactional
    public ResponseEntity<?> uploadPythonModel(
            @RequestParam("dslModel") MultipartFile dslModel,
            @RequestParam("modelName") String modelName,
            @RequestParam("modelOrderId") String modelOrderId) {
        try {

            // Basic null/empty file safety check
            if (dslModel == null || dslModel.isEmpty()) {
                return ResponseEntity.badRequest().body("Uploaded file is empty.");
            }

            // Check if model already exists
            Model model = modelService.getModelByName(modelName);
            if (model != null) {
                model.setIsDeleted(1);
                modelService.save(model);
            }

            // Upload file and save model
            // NOTE: Depending on your fileService implementation, you may need to pass
            // dslModel.getBytes() or dslModel.getInputStream() instead of the MultipartFile object
            String fileId = fileService.uploadPythonModelFile(dslModel);

            ModelConfig modelConfig = new ModelConfig();

            model = modelService.save(
                    modelName,
                    ModelType.DSL,
                    modelOrderId,
                    fileId,
                    Boolean.FALSE,
                    ModelStatus.INACTIVE,
                    new Date(),
                    "Fyntrac",
                    modelConfig
            );

            return ResponseEntity.ok(model);

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());

        } catch (Exception e) {
            String stackTrace = com.fyntrac.common.utils.StringUtil.getStackTrace(e);
            log.error(stackTrace);
            return ResponseEntity.internalServerError()
                    .body("An unexpected error occurred: " + e.getMessage());
        }
    }


    //Old Code
//    public ResponseEntity<?> uploadFile(@RequestParam("files") MultipartFile file,
//                                        @RequestParam("modelName") String modelName,
//                                        @RequestParam("modelOrderId") String modelOrderId) {
//        try {
//            if (!(modelService.ifModelExists(modelName)) && this.modelUploadService.validateModel(file)) {
//
//                String fileId = fileService.uploadFile(file);
//                ModelConfig modelConfig = new ModelConfig();
//                modelConfig.setMetrics(new Records.MetricNameRecord[]{});
//                modelConfig.setTransactions(new Records.TransactionNameRecord[]{});
//                modelConfig.setAggregationLevel(AggregationLevel.INSTRUMENT);
//                modelConfig.setCurrentVersion(Boolean.TRUE);
//                modelConfig.setLastOpenVersion(Boolean.FALSE);
//                modelConfig.setFirstVersion(Boolean.FALSE);
//                Model model = this.modelService.save(modelName
//                        , ModelType.EXCEL
//                        , modelOrderId
//                        , fileId
//                        , Boolean.FALSE
//                        , ModelStatus.CONFIGURE
//                        , new Date()
//                        , "Fyntrac"
//                        , modelConfig);
//                return ResponseEntity.ok(model);
//            } else {
//                return ResponseEntity.badRequest().body("Model Name already exists [" + modelName + "]");
//            }
//        } catch (IllegalArgumentException e) {
//            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
//        } catch (Exception e) {
//            String stackTrace = com.fyntrac.common.utils.StringUtil.getStackTrace(e);
//            log.error(stackTrace);
//            return ResponseEntity.internalServerError().body("An error occurred: " + e.getMessage());
//        }
//
//    }

    @PostMapping("/save")
    public ResponseEntity<String> save(@RequestBody Model m) {
        try {
            modelService.save(m);
            return ResponseEntity.ok("Model saved successfully, ID: " + m.getId());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("An error occurred: " + e.getMessage());
        }
    }

    @PostMapping("/configure")
    public ResponseEntity<?> configure(@RequestBody Model m) {
        try {
            Model model = modelService.save(m);
            return ResponseEntity.ok(model);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("An error occurred: " + e.getMessage());
        }
    }

    @PostMapping("/execute")
    public ResponseEntity<String> executeModel(@RequestBody Records.DateRequestRecord dateRequestRecord) {
        String tenant = TenantContextHolder.getTenant();
        if (!modelExecutionService.tryAcquireExecutionLock(tenant)) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("A model execution is already running for tenant [" + tenant + "]. Please wait for it to complete.");
        }
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
            Date executionDate = DateUtil.parseDate(dateRequestRecord.date(), formatter);
            int postingDate = DateUtil.dateInNumber(executionDate);

            // Clean up all derived data for this posting date before re-execution
           // this.modelExecutionService.cleanupDataForPostingDate(postingDate);

            // Streaming pipeline: generate events page-by-page and dispatch each batch
            // immediately. Only one page of instrument IDs lives in heap at a time.
            this.modelExecutionService.prepareExcelExecution(dateRequestRecord.date(), postingDate);
            excelModelService.generateEventAndDispatch(postingDate,
                    batch -> this.modelExecutionService.dispatchExcelBatch(executionDate, batch));
            this.modelExecutionService.finalizeExcelExecution(); // releases lock internally

            return ResponseEntity.ok("Model executed successfully, for : " + dateRequestRecord.date());
        } catch (IllegalArgumentException e) {
            modelExecutionService.releaseExecutionLock(tenant);
            log.error(StringUtil.getStackTrace(e));
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
        } catch (Exception e) {
            modelExecutionService.releaseExecutionLock(tenant);
            log.error(StringUtil.getStackTrace(e));
            return ResponseEntity.internalServerError().body("An error occurred: " + e.getMessage());
        } catch (Throwable e) {
            modelExecutionService.releaseExecutionLock(tenant);
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
        }
    }

    @PostMapping("/execute/dsl")
    public ResponseEntity<String> executeDslModel(@RequestBody Records.DateRequestRecord dateRequestRecord) {
        String tenant = TenantContextHolder.getTenant();
        if (!modelExecutionService.tryAcquireExecutionLock(tenant)) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("A model execution is already running for tenant [" + tenant + "]. Please wait for it to complete.");
        }
        try {
            log.info("dsl model execution requested for date: {}", dateRequestRecord.date());
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
            Date executionDate = DateUtil.parseDate(dateRequestRecord.date(), formatter);
            int postingDate = DateUtil.dateInNumber(executionDate);

            // Clean up all derived data for this posting date before re-execution
           // this.modelExecutionService.cleanupDataForPostingDate(postingDate);

            // Streaming pipeline: generate events page-by-page and dispatch each batch
            // immediately. Only one page of instrument IDs lives in heap at a time.
            this.modelExecutionService.preparePythonExecution(dateRequestRecord.date(), postingDate);
            excelModelService.generateEventAndDispatch(postingDate,
                    batch -> this.modelExecutionService.dispatchPythonBatch(executionDate, batch));
            this.modelExecutionService.finalizePythonExecution(); // releases lock internally

            // Update ExecutionState if executionDate < postingDate
            com.fyntrac.common.entity.ExecutionState executionState = executionStateService.getExecutionState();
            if (executionState != null
                    && executionState.getExecutionDate() != null
                    && executionState.getExecutionDate() < postingDate) {
                executionState.setLastExecutionDate(executionState.getExecutionDate());
                executionState.setExecutionDate(postingDate);
                executionStateService.update(executionState);
            }

            return ResponseEntity.ok("dsl model execution initiated for: " + dateRequestRecord.date());
        } catch (IllegalArgumentException e) {
            modelExecutionService.releaseExecutionLock(tenant);
            log.error(StringUtil.getStackTrace(e));
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
        } catch (Exception e) {
            modelExecutionService.releaseExecutionLock(tenant);
            log.error(StringUtil.getStackTrace(e));
            return ResponseEntity.internalServerError().body("An error occurred: " + e.getMessage());
        } catch (Throwable e) {
            modelExecutionService.releaseExecutionLock(tenant);
            log.error("dsl model execution error: {}", e.getMessage());
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
        }
    }


    // Download endpoint
    @GetMapping("/download/{fileId}")
    public ResponseEntity<byte[]> downloadFile(@PathVariable String fileId) {
        byte[] excelFile = fileService.getExcelFile(fileId);

        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"downloaded-file.xlsx\"")
                .body(excelFile);
    }

    @GetMapping("/get/all")
    public ResponseEntity<Collection<Model>> getAll() {
        try {
            Collection<Model> collection = modelService.getModels();
            return new ResponseEntity<>(collection, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * GET /api/dataloader/model/execution-summary
     *
     * On-demand aggregation of ModelExecutionBatchLog for the current tenant.
     * The UI can poll this endpoint every few seconds to watch real-time progress
     * as Python batches complete and write their own log documents.
     *
     * Query params:
     *   postingDate (optional) – YYYYMMDD integer. Falls back to ExecutionState.executionDate when absent.
     *   modelType   (optional) – "PYTHON" | "EXCEL". When absent, returns all model types.
     */
    @GetMapping("/execution-summary")
    public ResponseEntity<?> getExecutionSummary(
            @RequestParam(required = false) Integer postingDate,
            @RequestParam(required = false) String modelType) {
        try {
            // ── 1. Resolve tenantId from the current request context ────────────────
            String tenantId = TenantContextHolder.getTenant();
            if (tenantId == null || tenantId.isBlank()) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Tenant context is missing.");
            }

            // ── 2. Resolve postingDate (use ExecutionState if not supplied) ─────────
            if (postingDate == null) {
                com.fyntrac.common.entity.ExecutionState executionState = executionStateService.getExecutionState();
                if (executionState == null || executionState.getExecutionDate() == null) {
                    return ResponseEntity.status(HttpStatus.NOT_FOUND)
                            .body("No postingDate provided and no ExecutionState found for tenant: " + tenantId);
                }
                postingDate = executionState.getExecutionDate();
            }

            // ── 3. Fetch batch logs scoped to this tenant ───────────────────────────
            List<ModelExecutionBatchLog> logs =
                    modelExecutionBatchLogRepository.findByTenantIdAndPostingDateAndLogType(
                            tenantId, postingDate, "EXECUTION_BATCH");

            List<ModelExecutionBatchLog> summaryLogs =
                    modelExecutionBatchLogRepository.findByTenantIdAndPostingDateAndLogType(
                            tenantId, postingDate, "EXECUTION_SUMMARY");

            // Optional model-type filter
            if (modelType != null && !modelType.isBlank()) {
                final String modelTypeFilter = modelType.toUpperCase();
                logs = logs.stream()
                        .filter(l -> modelTypeFilter.equals(l.getModelType()))
                        .collect(Collectors.toList());
                summaryLogs = summaryLogs.stream()
                        .filter(l -> modelTypeFilter.equals(l.getModelType()))
                        .collect(Collectors.toList());
            }

            ModelExecutionBatchLog finalSummary = summaryLogs.stream()
                    .max(Comparator.comparing(l -> l.getCreatedAt() != null ? l.getCreatedAt() : new Date(0)))
                    .orElse(null);

            // ── 4. Aggregate totals ─────────────────────────────────────────────────
            int totalBatches     = logs.size();
            long sumBatchDuration = logs.stream().mapToLong(l -> l.getDurationMs() != null ? l.getDurationMs() : 0L).sum();
            
            int totalInstruments = finalSummary != null && finalSummary.getInstrumentCount() != null ? finalSummary.getInstrumentCount() : logs.stream().mapToInt(l -> l.getInstrumentCount() != null ? l.getInstrumentCount() : 0).sum();
            int totalSuccess     = finalSummary != null && finalSummary.getSuccessCount() != null ? finalSummary.getSuccessCount() : logs.stream().mapToInt(l -> l.getSuccessCount() != null ? l.getSuccessCount() : 0).sum();
            int totalFailed      = finalSummary != null && finalSummary.getFailedCount() != null ? finalSummary.getFailedCount() : logs.stream().mapToInt(l -> l.getFailedCount() != null ? l.getFailedCount() : 0).sum();
            long totalDurationMs = finalSummary != null && finalSummary.getDurationMs() != null ? finalSummary.getDurationMs() : sumBatchDuration;
            long avgBatchMs      = totalBatches > 0 ? sumBatchDuration / totalBatches : 0L;

            Map<String, Long> statusCounts = logs.stream()
                    .filter(l -> l.getStatus() != null)
                    .collect(Collectors.groupingBy(ModelExecutionBatchLog::getStatus, Collectors.counting()));

            List<String> errors = logs.stream()
                    .map(ModelExecutionBatchLog::getErrorMessage)
                    .filter(Objects::nonNull)
                    .filter(s -> !s.trim().isEmpty())
                    .collect(Collectors.toList());

            // ── 5. Per-batch breakdown (ordered by createdAt for UI timeline) ───────
            List<Map<String, Object>> batches = logs.stream()
                    .sorted(Comparator.comparing(
                            l -> l.getCreatedAt() != null ? l.getCreatedAt() : new Date(0)))
                    .map(l -> {
                        Map<String, Object> b = new LinkedHashMap<>();
                        b.put("batchNumber",     l.getBatchNumber());
                        b.put("jobId",           l.getJobId());
                        b.put("modelType",       l.getModelType());
                        b.put("instrumentCount", l.getInstrumentCount());
                        b.put("successCount",    l.getSuccessCount());
                        b.put("failedCount",     l.getFailedCount());
                        b.put("status",          l.getStatus());
                        b.put("durationMs",      l.getDurationMs());
                        b.put("errorMessage",    l.getErrorMessage());
                        b.put("createdAt",       l.getCreatedAt());
                        return b;
                    })
                    .collect(Collectors.toList());

            // ── 6. Build response ───────────────────────────────────────────────────
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("tenantId",        tenantId);
            summary.put("postingDate",     postingDate);
            summary.put("totalBatches",    totalBatches);
            summary.put("totalInstruments",totalInstruments);
            summary.put("totalSuccess",    totalSuccess);
            summary.put("totalFailed",     totalFailed);
            summary.put("totalDurationMs", totalDurationMs);
            summary.put("avgBatchMs",      avgBatchMs);
            summary.put("statusCounts",    statusCounts);
            summary.put("errors",          errors);
            summary.put("batches",         batches);   // per-batch breakdown for UI timeline

            return ResponseEntity.ok(summary);

        } catch (Exception e) {
            log.error("Failed to generate execution summary", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
        }
    }

    /**
     * GET /api/dataloader/model/execution-progress
     *
     * Returns real-time progress of model execution for the current posting date.
     * Uses EventHistory count / pageSize to derive total expected batches, then
     * compares against completed EXECUTION_BATCH log count for accurate % completion.
     *
     * Query params:
     *   postingDate (optional) – YYYYMMDD. Falls back to ExecutionState.executionDate.
     */
    @GetMapping("/execution-progress")
    public ResponseEntity<?> getExecutionProgress(
            @RequestParam(required = false) Integer postingDate) {
        try {
            String tenantId = TenantContextHolder.getTenant();
            if (tenantId == null || tenantId.isBlank()) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Tenant context is missing.");
            }

            if (postingDate == null) {
                com.fyntrac.common.entity.ExecutionState state = executionStateService.getExecutionState();
                if (state == null || state.getExecutionDate() == null) {
                    return ResponseEntity.status(HttpStatus.NOT_FOUND)
                            .body("No postingDate provided and no ExecutionState found for tenant: " + tenantId);
                }
                postingDate = state.getExecutionDate();
            }

            Map<String, Object> progress = modelExecutionService.getExecutionProgress(tenantId, postingDate, true);
            return ResponseEntity.ok(progress);

        } catch (Exception e) {
            log.error("Failed to compute execution progress", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
        }
    }
}
