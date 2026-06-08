package com.reserv.dataloader.controller;

import com.reserv.dataloader.exception.AccountingPeriodClosedException;
import com.reserv.dataloader.exception.MultiplePostingDatesException;
import com.reserv.dataloader.service.upload.FileUploadService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@Slf4j
@RestController
@RequestMapping("/api/dataloader/accounting/rule")
public class AccountingRuleController {

    @Autowired
    FileUploadService fileUploadService;

    @PostMapping("/upload")
    public ResponseEntity<String> handleFileUpload(@RequestParam("files") MultipartFile[] files) {
        try {
            // Process the uploaded files
            log.info("Tesing log");
            for (MultipartFile file : files) {
                // Save the file or perform any other operations
                System.out.println("Received file: " + file.getOriginalFilename());
                fileUploadService.uploadFiles(Boolean.FALSE, file);
            }
            return ResponseEntity.ok("Files uploaded successfully");
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
            for (MultipartFile file : files) {
                // Save the file or perform any other operations
                System.out.println("Received file: " + file.getOriginalFilename());
                fileUploadService.uploadFiles(Boolean.TRUE, file);
            }
            return ResponseEntity.ok("Files uploaded successfully");
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

}
