package com.reserv.dataloader.controller;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.enums.CustomTableType;
import com.fyntrac.common.service.CustomTableDefinitionService;
import com.fyntrac.common.service.DataService;
import com.reserv.dataloader.service.upload.FileUploadService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/dataloader/fyntrac/custom-table")
@Slf4j
public class CustomTableController {

    private final CustomTableDefinitionService tableDefinitionService;
    private final DataService dataService;
    // Add these methods to your TableDefinitionController.java

    @Autowired
    public  CustomTableController(CustomTableDefinitionService tableDefinitionService,
                                  DataService dataService) {
            this.tableDefinitionService = tableDefinitionService;
            this.dataService = dataService;
    }
    @PostMapping("/create-with-physical")
    public ResponseEntity<Records.ApiResponseRecord<CustomTableDefinition>> createTableWithPhysicalCollection(
            @Valid @RequestBody Records.CustomTableRequestRecord request) {
        try {
            CustomTableDefinition createdTable = tableDefinitionService.createTableWithPhysicalTable(request);
            return ResponseEntity.ok(Records.ApiResponseRecord.success("Table and physical collection created successfully", createdTable));

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Records.ApiResponseRecord.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                    .body(Records.ApiResponseRecord.error("Failed to create table and physical collection: " + e.getMessage()));
        }
    }

    @PostMapping("/{id}/create-physical")
    public ResponseEntity<Records.ApiResponseRecord<Boolean>> createPhysicalCollection(@PathVariable String id) {
        try {
            CustomTableDefinition tableDefinition = tableDefinitionService.findById(id)
                    .orElseThrow(() -> new IllegalArgumentException("Table definition not found"));

            tableDefinitionService.createPhysicalTable(tableDefinition);
            return ResponseEntity.ok(Records.ApiResponseRecord.success("Physical collection created successfully",
                    Boolean.TRUE));

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Records.ApiResponseRecord.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                    .body(Records.ApiResponseRecord.error("Failed to create physical collection: " + e.getMessage()));
        }
    }

    @GetMapping("/get/{id}")
    public ResponseEntity<Records.ApiResponseRecord<CustomTableDefinition>> getCustomTableById(@PathVariable String id) {
        try {
            CustomTableDefinition tableDefinition = tableDefinitionService.findById(id)
                    .orElseThrow(() -> new IllegalArgumentException("Table definition not found"));

            return ResponseEntity.ok(Records.ApiResponseRecord.success("Custom table find successfully",
                    tableDefinition));

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Records.ApiResponseRecord.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                    .body(Records.ApiResponseRecord.error("Failed to get custom table: " + e.getMessage()));
        }
    }

    @DeleteMapping("/{id}/physical")
    public ResponseEntity<Records.ApiResponseRecord<Boolean>> dropPhysicalCollection(@PathVariable String id) {
        try {
            CustomTableDefinition tableDefinition = tableDefinitionService.findById(id)
                    .orElseThrow(() -> new IllegalArgumentException("Table definition not found"));

            tableDefinitionService.dropPhysicalTable(tableDefinition.getTableName());
            return ResponseEntity.ok(Records.ApiResponseRecord.success("Physical collection dropped successfully",
                    Boolean.TRUE));

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Records.ApiResponseRecord.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                    .body(Records.ApiResponseRecord.error("Failed to drop physical collection: " + e.getMessage()));
        }
    }

    @GetMapping("/{id}/physical/exists")
    public ResponseEntity<Records.ApiResponseRecord<Boolean>> checkPhysicalCollectionExists(@PathVariable String id) {
        try {
            CustomTableDefinition tableDefinition = tableDefinitionService.findById(id)
                    .orElseThrow(() -> new IllegalArgumentException("Table definition not found"));

            boolean exists = tableDefinitionService.physicalTableExists(tableDefinition.getTableName());
            return ResponseEntity.ok(Records.ApiResponseRecord.success(exists));

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Records.ApiResponseRecord.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                    .body(Records.ApiResponseRecord.error("Failed to check physical collection: " + e.getMessage()));
        }
    }

    @GetMapping("/{id}/physical/stats")
    public ResponseEntity<Records.ApiResponseRecord<Map<String, Object>>> getPhysicalCollectionStats(@PathVariable String id) {
        try {
            CustomTableDefinition tableDefinition = tableDefinitionService.findById(id)
                    .orElseThrow(() -> new IllegalArgumentException("Table definition not found"));

            Map<String, Object> stats = tableDefinitionService.getTableStats(tableDefinition.getTableName());
            return ResponseEntity.ok(Records.ApiResponseRecord.success(stats));

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Records.ApiResponseRecord.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                    .body(Records.ApiResponseRecord.error("Failed to get collection stats: " + e.getMessage()));
        }
    }

    @GetMapping("/reference-tables")
    public ResponseEntity<Records.ApiResponseRecord<Optional<List<CustomTableDefinition>>>> getReferenceTable() {
        try {
            Optional<List<CustomTableDefinition>> referenceTables =
                    Optional.ofNullable(tableDefinitionService.getCustomTables(CustomTableType.REFERENCE)
                            .orElseThrow(() -> new IllegalArgumentException("Table definition not found")));

            return ResponseEntity.ok(Records.ApiResponseRecord.success(referenceTables));

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Records.ApiResponseRecord.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                    .body(Records.ApiResponseRecord.error("Failed to get reference tables: " + e.getMessage()));
        }
    }

    @GetMapping("/operational-tables")
    public ResponseEntity<Records.ApiResponseRecord<Optional<List<CustomTableDefinition>>>> getOperationalTable() {
        try {
            Optional<List<CustomTableDefinition>> referenceTables =
                    Optional.ofNullable(tableDefinitionService.getCustomTables(CustomTableType.OPERATIONAL)
                            .orElseThrow(() -> new IllegalArgumentException("Table definition not found")));

            return ResponseEntity.ok(Records.ApiResponseRecord.success(referenceTables));

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Records.ApiResponseRecord.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                    .body(Records.ApiResponseRecord.error("Failed to get reference tables: " + e.getMessage()));
        }
    }

    @GetMapping("/get/all-tables")
    public ResponseEntity<Records.ApiResponseRecord<List<String>>> getAllExistingTables() {
        try {
            List<String> tables =
                    this.dataService.getUserCollectionNames();

            return ResponseEntity.ok(Records.ApiResponseRecord.success(tables));

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Records.ApiResponseRecord.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                    .body(Records.ApiResponseRecord.error("Failed to get  tables list : " + e.getMessage()));
        }
    }

    @Autowired
    FileUploadService fileUploadService;
    @PostMapping("/data-upload")
    public ResponseEntity<String> handleFileUpload(@RequestParam("files") MultipartFile[] files) {
        try {
            // Process the uploaded files
            log.info("Tesing log");
            for (MultipartFile file : files) {
                // Save the file or perform any other operations
                System.out.println("Received file: " + file.getOriginalFilename());
                fileUploadService.uploadCustomTableDataFiles(file);
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
}
