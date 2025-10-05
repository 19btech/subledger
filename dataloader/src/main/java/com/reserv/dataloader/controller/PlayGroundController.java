package com.reserv.dataloader.controller;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.DataFiles;
import com.fyntrac.common.enums.DataFileType;
import com.reserv.dataloader.pulsar.producer.CommonMessageProducer;
import com.reserv.dataloader.service.upload.TestDataFileUploadService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/api/dataloader/model/play-ground")
public class PlayGroundController {

    @Autowired
    TestDataFileUploadService fileUploadService;

    @Autowired
    CommonMessageProducer commonMessageProducer;

    @PostMapping("/test-data/upload")
    public ResponseEntity<String> handleTestDataFileUpload(@RequestParam("files") MultipartFile[] files) {
        try {
            // Process the uploaded files
            log.info("Tesing log");
            for (MultipartFile file : files) {
                // Save the file or perform any other operations
                System.out.println("Received file: " + file.getOriginalFilename());
                fileUploadService.uploadFiles(file);
            }


            return ResponseEntity.ok("Files uploaded successfully");
        } catch (Exception e) {
            String stackTrace = com.fyntrac.common.utils.StringUtil.getStackTrace(e);
            log.error(stackTrace);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to upload files: cause:" + stackTrace);
        } catch (Throwable e) {
            log.error(e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }

    @GetMapping("/get/ref-data-files")
    public ResponseEntity<Collection<Records.DataFileRecord>> getRefDataFiles() {
        try {
            Collection<Records.DataFileRecord> collection = fileUploadService.getDataFileRecords(DataFileType.REFERENCE_DATA);
            return new ResponseEntity<>(collection, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/activity-data-files")
    public ResponseEntity<Collection<Records.DataFileRecord>> getActivityDataFiles() {
        try {
            Collection<Records.DataFileRecord> collection = fileUploadService.getDataFileRecords(DataFileType.ACTIVITY_DATA);
            return new ResponseEntity<>(collection, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/output-data-files")
    public ResponseEntity<Collection<Records.DataFileRecord>> getOutputDataFiles() {
        try {
            Collection<Records.DataFileRecord> collection = fileUploadService.getDataFileRecords(DataFileType.OUTPUT_DATA);
            return new ResponseEntity<>(collection, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/project-data-files")
    public ResponseEntity<Collection<Records.DataFileRecord>> getProjectDataFiles() {
        try {
            Collection<Records.DataFileRecord> collection = fileUploadService.getDataFileRecords(DataFileType.PROJECT_FILE_DATA);
            return new ResponseEntity<>(collection, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/download/file")
    public ResponseEntity<byte[]> downloadFile(@RequestBody Records.DataFileRecord df) {
        try {
            if (df == null || df.id() == null || df.id().isBlank()) {
                // Invalid request body
                return ResponseEntity.badRequest().build();
            }

            Optional<DataFiles> optionalFile = fileUploadService.getDataFileById(df.id());

            if (optionalFile.isEmpty()) {
                // File not found
                return ResponseEntity.notFound().build();
            }

            DataFiles dataFile = optionalFile.get();
            byte[] fileContent = dataFile.getContent();
            String fileName = dataFile.getFileName();

            if (fileContent == null || fileName == null) {
                // Bad data in DB
                return ResponseEntity.unprocessableEntity().build();
            }

            // Determine Content Type
            String contentType = dataFile.getContentType();
            if (contentType == null || contentType.isBlank()) {
                contentType = guessContentTypeFromExtension(fileName);
            }

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.parseMediaType(contentType));
            headers.setContentDisposition(
                    ContentDisposition.attachment()
                            .filename(fileName)
                            .build()
            );
            headers.setContentLength(fileContent.length);

            return new ResponseEntity<>(fileContent, headers, HttpStatus.OK);

        } catch (IllegalArgumentException e) {
            // Invalid content type or other argument errors
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            // Any unexpected errors
            // log.error("File download failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // Utility method to guess content type based on file extension
    private String guessContentTypeFromExtension(String fileName) {
        if (fileName == null) return MediaType.APPLICATION_OCTET_STREAM_VALUE;
        String lower = fileName.toLowerCase();
        if (lower.endsWith(".csv")) {
            return "text/csv";
        } else if (lower.endsWith(".xls")) {
            return "application/vnd.ms-excel";
        } else if (lower.endsWith(".xlsx")) {
            return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
        } else {
            return MediaType.APPLICATION_OCTET_STREAM_VALUE; // default fallback
        }
    }

    @PostMapping("/execute-model")
    public ResponseEntity<String> executeModel() {
        try {
            // Process the uploaded files
            log.info("Tesing log");

            String[] instruments = {"S01"};
            String[] models = null;
            Records.InstrumentMessageRecord instrumentMessageRec =
                    RecordFactory.CreateInstrumentMessageRecord(TenantContextHolder.getTenant(),
                    instruments, models );

            commonMessageProducer.sendModelExecutionMessage(instrumentMessageRec);

            return ResponseEntity.ok("Files uploaded successfully");
        } catch (Exception e) {
            String stackTrace = com.fyntrac.common.utils.StringUtil.getStackTrace(e);
            log.error(stackTrace);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to upload files: cause:" + stackTrace);
        } catch (Throwable e) {
            log.error(e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }
}
