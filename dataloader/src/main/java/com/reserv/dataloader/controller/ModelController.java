package com.reserv.dataloader.controller;

import com.reserv.dataloader.service.ExcelFileService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import com.fyntrac.common.service.ModelConfigurationService;
import com.fyntrac.common.enums.ModelStatus;
import com.fyntrac.common.entity.Model;

import java.util.Collection;
import java.util.Date;

@RestController
@RequestMapping("/api/dataloader/model")
@Slf4j
public class ModelController {

    private final ExcelFileService fileService;
    private final ModelConfigurationService modelConfigurationService;

    @Autowired
    public ModelController(ExcelFileService fileService
                            , ModelConfigurationService modelConfigurationServicen) {
        this.fileService = fileService;
        this.modelConfigurationService = modelConfigurationServicen;
    }
    // Upload endpoint
    @PostMapping("/upload")
    @Transactional
    public ResponseEntity<String> uploadFile(@RequestParam("files") MultipartFile file,
                                             @RequestParam("modelName") String modelName,
                                             @RequestParam("modelOrderId") String modelOrderId) {
        try {
            String fileId = fileService.uploadFile(file);
            this.modelConfigurationService.save(modelName, modelOrderId, fileId, Boolean.FALSE, ModelStatus.CONFIGURE, new Date(), "Fyntrac");
            return ResponseEntity.ok("File uploaded successfully, ID: " + fileId);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("An error occurred: " + e.getMessage());
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
            Collection<Model> collection = modelConfigurationService.getModels();
            return new ResponseEntity<>(collection, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
