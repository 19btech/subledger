package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.ModelConfig;
import com.fyntrac.common.enums.AttributeVersion;
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
import com.fyntrac.common.service.ModelService;
import com.fyntrac.common.enums.ModelStatus;
import com.fyntrac.common.entity.Model;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.Date;

@RestController
@RequestMapping("/api/dataloader/model")
@Slf4j
public class ModelController {

    private final ExcelFileService fileService;
    private final ModelService modelService;

    @Autowired
    public ModelController(ExcelFileService fileService
                            , ModelService modelServicen) {
        this.fileService = fileService;
        this.modelService = modelServicen;
    }
    // Upload endpoint
    @PostMapping("/upload")
    @Transactional
    public ResponseEntity<String> uploadFile(@RequestParam("files") MultipartFile file,
                                             @RequestParam("modelName") String modelName,
                                             @RequestParam("modelOrderId") String modelOrderId) {
        try {
            if (!(modelService.ifModelExists(modelName))) {
                String fileId = fileService.uploadFile(file);
                ModelConfig modelConfig = new ModelConfig();
                modelConfig.setMetrics(null);
                modelConfig.setTransactions(null);
                modelConfig.setAggregationLevel(null);
                modelConfig.setCurrentVersion(Boolean.FALSE);
                modelConfig.setLastOpenVersion(Boolean.FALSE);
                modelConfig.setFirstVersion(Boolean.FALSE);
                this.modelService.save(modelName
                        , modelOrderId
                        , fileId
                        , Boolean.FALSE
                        , ModelStatus.CONFIGURE
                        , new Date()
                        , "Fyntrac"
                        , modelConfig);
                return ResponseEntity.ok("File uploaded successfully, ID: " + fileId);
            }else{
                return ResponseEntity.badRequest().body("Model Name already exists [" + modelName + "]");
            }
            } catch(IllegalArgumentException e){
                return ResponseEntity.badRequest().body("Error: " + e.getMessage());
            } catch(Exception e){
                return ResponseEntity.internalServerError().body("An error occurred: " + e.getMessage());
            }

    }

    @PostMapping("/save")
    public ResponseEntity<String> save(@RequestBody Model m) {
        try {
            modelService.save(m);
            return ResponseEntity.ok("Model saved successfully, ID: " + m.getId());
        }catch (IllegalArgumentException e) {
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
            Collection<Model> collection = modelService.getModels();
            return new ResponseEntity<>(collection, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
