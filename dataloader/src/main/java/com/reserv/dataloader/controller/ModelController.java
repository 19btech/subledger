package com.reserv.dataloader.controller;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.ModelConfig;
import com.fyntrac.common.enums.AggregationLevel;
import com.fyntrac.common.utils.StringUtil;
import com.reserv.dataloader.service.DataloaderExcelFileService;
import com.reserv.dataloader.service.ModelUploadService;
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
import com.fyntrac.common.service.ModelService;
import com.fyntrac.common.enums.ModelStatus;
import com.fyntrac.common.entity.Model;

import java.util.Collection;
import java.util.Date;

@RestController
@RequestMapping("/api/dataloader/model")
@Slf4j
public class ModelController {

    private final DataloaderExcelFileService fileService;
    private final ModelService modelService;
    private final ModelUploadService modelUploadService;
    private final ModelExecutionService modelExecutionService;


    @Autowired
    public ModelController(DataloaderExcelFileService fileService
                            , ModelService modelServicen
                            , ModelUploadService modelUploadService
                            , ModelExecutionService modelExecutionService) {
        this.fileService = fileService;
        this.modelService = modelServicen;
        this.modelUploadService = modelUploadService;
        this.modelExecutionService = modelExecutionService;
    }
    // Upload endpoint
    @PostMapping("/upload")
    @Transactional
    public ResponseEntity<?> uploadFile(@RequestParam("files") MultipartFile file,
                                             @RequestParam("modelName") String modelName,
                                             @RequestParam("modelOrderId") String modelOrderId) {
        try {
            if (!(modelService.ifModelExists(modelName)) && this.modelUploadService.validateModel(file)) {

                String fileId = fileService.uploadFile(file);
                ModelConfig modelConfig = new ModelConfig();
                modelConfig.setMetrics(new Records.MetricNameRecord[]{});
                modelConfig.setTransactions(new Records.TransactionNameRecord[]{});
                modelConfig.setAggregationLevel(AggregationLevel.INSTRUMENT);
                modelConfig.setCurrentVersion(Boolean.TRUE);
                modelConfig.setLastOpenVersion(Boolean.FALSE);
                modelConfig.setFirstVersion(Boolean.FALSE);
                Model model = this.modelService.save(modelName
                        , modelOrderId
                        , fileId
                        , Boolean.FALSE
                        , ModelStatus.CONFIGURE
                        , new Date()
                        , "Fyntrac"
                        , modelConfig);
                return ResponseEntity.ok(model);
            }else{
                return ResponseEntity.badRequest().body("Model Name already exists [" + modelName + "]");
            }
            } catch(IllegalArgumentException e){
                return ResponseEntity.badRequest().body("Error: " + e.getMessage());
            } catch(Exception e){
            String stackTrace = com.fyntrac.common.utils.StringUtil.getStackTrace(e);
            log.error(stackTrace);
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

    @PostMapping("/configure")
    public ResponseEntity<?> configure(@RequestBody Model m) {
        try {
            Model model = modelService.save(m);
            return ResponseEntity.ok(model);
        }catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("An error occurred: " + e.getMessage());
        }
    }

    @PostMapping("/execute")
    public ResponseEntity<String> executeModel(@RequestBody Records.DateRequestRecord dateRequestRecord) {
        try {
            this.modelExecutionService.sendModelExecutionMessage(dateRequestRecord.date());
            return ResponseEntity.ok("Model executed successfully, for : " + dateRequestRecord.date());
        }catch (IllegalArgumentException e) {
            log.error(StringUtil.getStackTrace(e));
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
        } catch (Exception e) {
            log.error(StringUtil.getStackTrace(e));
            return ResponseEntity.internalServerError().body("An error occurred: " + e.getMessage());
        } catch (Throwable e) {
            // log.error(StringUtil.getStackTrace(e));
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
}
