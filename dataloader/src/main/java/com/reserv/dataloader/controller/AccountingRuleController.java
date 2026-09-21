package com.reserv.dataloader.controller;

import com.fyntrac.common.config.TenantContextHolder;
import com.reserv.dataloader.exception.AccountingPeriodClosedException;
import com.reserv.dataloader.exception.MultiplePostingDatesException;
import com.reserv.dataloader.service.UploadStatusService;
import com.reserv.dataloader.service.upload.AsyncUploadOrchestrator;
import com.reserv.dataloader.service.upload.FileUploadService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;
import java.util.Set;

@Slf4j
@RestController
@RequestMapping("/api/dataloader/accounting/rule")
public class AccountingRuleController {

    @Autowired
    FileUploadService fileUploadService;

    @Autowired
    UploadStatusService uploadStatusService;

    @Autowired
    AsyncUploadOrchestrator asyncUploadOrchestrator;

    @PostMapping("/upload")
    public ResponseEntity<String> handleFileUpload(@RequestParam("files") MultipartFile[] files) {
        try {
            // Process the uploaded files
            log.info("Tesing log");
            long uploadId = 0;
            for (MultipartFile file : files) {
                // Save the file or perform any other operations
                System.out.println("Received file: " + file.getOriginalFilename());
                uploadId = fileUploadService.uploadFiles(Boolean.FALSE, file);
            }
            return ResponseEntity.ok("Files uploaded successfully. uploadId=" + uploadId);
        } catch (AccountingPeriodClosedException e) {
            // Validation check 1: executionDate falls in a closed accounting period
            log.warn("Upload rejected – accounting period is closed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Upload rejected: " + e.getMessage());
        } catch (MultiplePostingDatesException e) {
            log.warn("Upload rejected – multiple posting dates: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Upload rejected: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            // Validation check 2: postingDate in uploaded file is earlier than executionDate
            log.warn("Upload rejected – postingDate validation failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body("Upload rejected: " + e.getMessage());
        } catch (Exception e) {
            String stackTrace = com.fyntrac.common.utils.StringUtil.getStackTrace(e);
            log.error(stackTrace);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to upload files: cause:" + stackTrace);
        } catch (Throwable e) {
            log.error(e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }

    @PostMapping("/upload-overwrite")
    public ResponseEntity<String> handleFileUploadOverwrite(@RequestParam("files") MultipartFile[] files) {
        try {
            // Process the uploaded files
            log.info("Tesing log");
            long uploadId = 0;
            for (MultipartFile file : files) {
                // Save the file or perform any other operations
                System.out.println("Received file: " + file.getOriginalFilename());
                uploadId = fileUploadService.uploadFiles(Boolean.TRUE, file);
            }
            return ResponseEntity.ok("Files uploaded successfully. uploadId=" + uploadId);
        } catch (AccountingPeriodClosedException e) {
            // Validation check 1: executionDate falls in a closed accounting period
            log.warn("Upload rejected – accounting period is closed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(e.getMessage());
        } catch (MultiplePostingDatesException e) {
            log.warn("Upload rejected – multiple posting dates: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(e.getMessage());
        } catch (IllegalArgumentException e) {
            // Validation check 2: postingDate in uploaded file is earlier than executionDate
            log.warn("Upload rejected – postingDate validation failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body(e.getMessage());
        } catch (Exception e) {
            String stackTrace = com.fyntrac.common.utils.StringUtil.getStackTrace(e);
            log.error(stackTrace);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to upload files: cause:" + stackTrace);
        } catch (Throwable e) {
            log.error(e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Async counterpart to {@code /upload}: stages the files synchronously (required — see
     * {@link FileUploadService#stageMultipartFiles}), then hands CSV conversion / validation /
     * batch-job execution to a background executor and returns immediately with a 202 + uploadId
     * instead of blocking the request until the whole pipeline finishes. Poll
     * {@code GET /api/dataloader/upload/status/{uploadId}} for completion.
     * See docs/K8S_SCALING_STRATEGY.md (Stage 0).
     */
    @PostMapping("/upload-async")
    public ResponseEntity<?> handleFileUploadAsync(@RequestParam("files") MultipartFile[] files) {
        return submitAsyncUpload(Boolean.FALSE, files);
    }

    @PostMapping("/upload-overwrite-async")
    public ResponseEntity<?> handleFileUploadOverwriteAsync(@RequestParam("files") MultipartFile[] files) {
        return submitAsyncUpload(Boolean.TRUE, files);
    }

    private ResponseEntity<?> submitAsyncUpload(boolean isOverwrite, MultipartFile[] files) {
        try {
            long uploadId = FileUploadService.generateUploadId();
            Set<String> validFileSet = fileUploadService.stageMultipartFiles(files);
            String tenant = TenantContextHolder.getTenant();

            uploadStatusService.markPending(uploadId, isOverwrite);
            asyncUploadOrchestrator.processAsync(uploadId, isOverwrite, validFileSet, tenant);

            return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                    "uploadId", uploadId,
                    "state", "PENDING",
                    "statusUrl", "/api/dataloader/upload/status/" + uploadId
            ));
        } catch (Throwable e) {
            String stackTrace = com.fyntrac.common.utils.StringUtil.getStackTrace(e);
            log.error(stackTrace);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to stage files for upload: cause:" + stackTrace);
        }
    }

}
