package com.reserv.dataloader.controller;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.CustomTableColumn;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.entity.Option;
import com.fyntrac.common.enums.CustomTableType;
import com.fyntrac.common.service.CustomTableDefinitionService;
import com.fyntrac.common.service.DataService;
import com.reserv.dataloader.service.upload.FileUploadService;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.Valid;
import java.util.*;
import java.util.stream.Collectors;

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

    @GetMapping("/get/all/operational-tables/options")
    public ResponseEntity<Records.ApiResponseRecord<Collection<Records.CustomTableColumnsRecord>>>
    getOperationalTableOptions() {

        try {
            List<CustomTableDefinition> operationalTables =
                    tableDefinitionService
                            .getCustomTables(CustomTableType.OPERATIONAL)
                            .orElseThrow(() -> new IllegalArgumentException("Table definition not found"));

            Collection<Records.CustomTableColumnsRecord> options =
                    operationalTables.stream()
                            .map(table -> {

                                // ✅ Safely extract column names as List<String>
                                List<String> columnNames = Optional.ofNullable(table.getColumns())
                                        .orElse(List.of())
                                        .stream()
                                        .filter(Objects::nonNull)
                                        .map(CustomTableColumn::getColumnName)
                                        .filter(Objects::nonNull)
                                        .toList();

                                // ✅ Build record with tableName + columns
                                return RecordFactory.creatCustomTableColumnsRecord(
                                        table.getTableName(),
                                        columnNames
                                );
                            })
                            .collect(Collectors.toList());

            return ResponseEntity.ok(Records.ApiResponseRecord.success(options));

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .body(Records.ApiResponseRecord.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Records.ApiResponseRecord.error(
                            "Failed to get operational tables: " + e.getMessage()
                    ));
        }
    }


    @GetMapping("/get/all/reference-tables/options")
    public ResponseEntity<Records.ApiResponseRecord<Collection<Records.CustomTableColumnsRecord>>>
    getReferenceTableOptions() {

        try {
            List<CustomTableDefinition> operationalTables =
                    tableDefinitionService
                            .getCustomTables(CustomTableType.REFERENCE)
                            .orElseThrow(() -> new IllegalArgumentException("Table definition not found"));

            Collection<Records.CustomTableColumnsRecord> options =
                    operationalTables.stream()
                            .map(table -> {

                                // ✅ Safely extract column names as List<String>
                                List<String> columnNames = Optional.ofNullable(table.getColumns())
                                        .orElse(List.of())
                                        .stream()
                                        .filter(Objects::nonNull)
                                        .map(CustomTableColumn::getColumnName)
                                        .filter(Objects::nonNull)
                                        .toList();

                                // ✅ Build record with tableName + columns
                                return RecordFactory.creatCustomTableColumnsRecord(
                                        table.getTableName(),
                                        columnNames
                                );
                            })
                            .collect(Collectors.toList());

            return ResponseEntity.ok(Records.ApiResponseRecord.success(options));

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .body(Records.ApiResponseRecord.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Records.ApiResponseRecord.error(
                            "Failed to get operational tables: " + e.getMessage()
                    ));
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

    @GetMapping("/get/values/operational_table/{reference}")
    public ResponseEntity<Records.ApiResponseRecord<List<Option>>> getOperationalTableValues(@PathVariable String reference) {
        try {
            CustomTableDefinition tableDefinition = tableDefinitionService.getCustomTableDefinition(reference);

            // 1. Check if definition exists
            if (tableDefinition == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Records.ApiResponseRecord.error("Table definition not found for: " + reference));
            }

            // 2. Check if it is the correct type
            if (tableDefinition.getTableType() != CustomTableType.OPERATIONAL) {
                return ResponseEntity.badRequest()
                        .body(Records.ApiResponseRecord.error("Table '" + reference + "' is not of type OPERATIONAL."));
            }

            // 3. Perform logic
            String referenceTable = tableDefinition.getReferenceTable();

            // Optional safety check if referenceTable can be null
            if (referenceTable == null || referenceTable.isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(Records.ApiResponseRecord.error("No Reference Table linked to Operational Table: " + reference));
            }

            List<Option> options = new ArrayList<>();

            if(tableDefinition.getPrimaryKeys() != null && !tableDefinition.getPrimaryKeys().isEmpty()) {
                String primaryKey = tableDefinition.getPrimaryKeys().get(0);

                List< Document> data = this.dataService.findSelectedFieldsAsMap(referenceTable,List.of(primaryKey));

                for (Document doc : data) {
                    // Extract the value using the field name.
                    // We cast to String, or use toString() to be safe if it's an ObjectId or Integer.
                    Object rawValue = doc.get(primaryKey);

                    if (rawValue != null) {
                        String val = rawValue.toString();

                        // Create the Option object (Label = Value in this case)
                        Option option = Option.builder()
                                .label(val)
                                .value(val)
                                .build();

                        options.add(option);
                    }
                }
            }
            return ResponseEntity.ok(Records.ApiResponseRecord.success(options));

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Records.ApiResponseRecord.error(e.getMessage()));
        } catch (Exception e) {
            // Using Internal Server Error (500) for unexpected exceptions is generally better practice than Bad Request (400)
            return ResponseEntity.internalServerError()
                    .body(Records.ApiResponseRecord.error("Failed to get table stats: " + e.getMessage()));
        }
    }


    @GetMapping("/get/values/reference_table/{reference}")
    public ResponseEntity<Records.ApiResponseRecord<List<Option>>> getReferenceTableValues(@PathVariable String reference) {
        try {
            CustomTableDefinition tableDefinition = tableDefinitionService.getCustomTableDefinition(reference);

            // 1. Check if definition exists
            if (tableDefinition == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Records.ApiResponseRecord.error("Table definition not found for: " + reference));
            }

            // 2. Check if it is the correct type
            if (tableDefinition.getTableType() != CustomTableType.REFERENCE) {
                return ResponseEntity.badRequest()
                        .body(Records.ApiResponseRecord.error("Table '" + reference + "' is not of type OPERATIONAL."));
            }

            // 3. Perform logic
            String referenceTable = tableDefinition.getReferenceTable();

            // Optional safety check if referenceTable can be null
            if (referenceTable == null || referenceTable.isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(Records.ApiResponseRecord.error("No Reference Table linked to Operational Table: " + reference));
            }

            List<Option> options = new ArrayList<>();

            if(tableDefinition.getPrimaryKeys() != null && !tableDefinition.getPrimaryKeys().isEmpty()) {
                String primaryKey = tableDefinition.getPrimaryKeys().get(0);

                List< Document> data = this.dataService.findSelectedFieldsAsMap(referenceTable,List.of(primaryKey));

                for (Document doc : data) {
                    // Extract the value using the field name.
                    // We cast to String, or use toString() to be safe if it's an ObjectId or Integer.
                    Object rawValue = doc.get(primaryKey);

                    if (rawValue != null) {
                        String val = rawValue.toString();

                        // Create the Option object (Label = Value in this case)
                        Option option = Option.builder()
                                .label(val)
                                .value(val)
                                .build();

                        options.add(option);
                    }
                }
            }
            return ResponseEntity.ok(Records.ApiResponseRecord.success(options));

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Records.ApiResponseRecord.error(e.getMessage()));
        } catch (Exception e) {
            // Using Internal Server Error (500) for unexpected exceptions is generally better practice than Bad Request (400)
            return ResponseEntity.internalServerError()
                    .body(Records.ApiResponseRecord.error("Failed to get table stats: " + e.getMessage()));
        }
    }
}
