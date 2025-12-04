package com.fyntrac.reporting.service;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.CustomTableColumn;
import com.fyntrac.common.entity.Option;
import com.fyntrac.common.enums.CustomTableType;
import com.fyntrac.common.repository.CustomTableDefinitionRepository;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.utils.MongoQueryGenerator;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
@Slf4j
@Service
public class CustomOperationalDataReportingService {

    private final CustomTableDefinitionRepository customTableDefinitionRepository;
    private final DataService<?> dataService;
    public CustomOperationalDataReportingService(CustomTableDefinitionRepository customTableDefinitionRepository,
                                         DataService<?> dataService) {
        this.customTableDefinitionRepository = customTableDefinitionRepository;
        this.dataService = dataService;
    }


    public List<Option> getRefDataTables() {
        return customTableDefinitionRepository
                .findByTableType(CustomTableType.OPERATIONAL)
                .orElse(List.of()) // ✅ Prevents NPE
                .stream()
                .filter(Objects::nonNull)
                .map(table -> {
                    Option option = new Option();
                    option.setLabel(table.getTableName());       // ✅ label = tableName
                    option.setValue(String.valueOf(table.getId())); // ✅ value = id
                    return option;
                })
                .toList(); // ✅ Java 21 immutable list
    }



    public Map<String, List<String>> getRefDataTableColumns(String id) {
        return customTableDefinitionRepository
                .findById(id)
                .map(table -> {
                    String tableName = table.getTableName();

                    List<String> columnNames = table.getColumns()
                            .stream()
                            .filter(Objects::nonNull)
                            .map(CustomTableColumn::getColumnName)
                            .filter(Objects::nonNull)
                            .toList(); // ✅ Java 21 immutable list

                    return Map.of(tableName, columnNames); // ✅ tableName -> List<columns>
                })
                .orElse(Map.of()); // ✅ Safe fallback if not found
    }


    public List<Document> executeReport(String id, List<Records.QueryCriteriaItem> queryCriteria) {

        return customTableDefinitionRepository
                .findById(id)
                .map(table -> {

                    // ✅ Safe column extraction
                    List<CustomTableColumn> columns = Optional.ofNullable(table.getColumns()).orElse(List.of());

                    // ✅ Collect projection fields, always include "_id"
                    List<String> fields = columns.stream()
                            .filter(Objects::nonNull)
                            .map(CustomTableColumn::getColumnName)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toCollection(ArrayList::new));

                    if (!fields.contains("_id")) {
                        fields.add("_id");
                    }

                    // ✅ Generate aggregation pipeline Documents
                    List<Document> pipelineDocs = MongoQueryGenerator.generateAggregationPipeline(queryCriteria, fields);
                    log.info("Custom ref data table Aggregation Pipeline: {}", pipelineDocs);

                    // ✅ Convert Documents to proper AggregationOperations
                    List<AggregationOperation> operations = pipelineDocs.stream()
                            .map(doc -> (AggregationOperation) context -> doc)
                            .toList();

                    // ✅ Execute aggregation
                    List<Document> result = dataService.getMongoTemplate()
                            .aggregate(newAggregation(operations), table.getTableName(), Document.class)
                            .getMappedResults();

                    // ✅ Map "_id" to "id" as string for MUI DataGrid
                    return result.stream()
                            .map(doc -> {
                                if (doc.containsKey("_id")) {
                                    Object idValue = doc.get("_id");
                                    // Convert ObjectId to string if needed
                                    if (idValue instanceof org.bson.types.ObjectId objectId) {
                                        doc.put("id", objectId.toHexString());
                                    } else {
                                        doc.put("id", idValue.toString());
                                    }
                                }
                                return doc;
                            })
                            .toList();

                })
                .orElseGet(List::of); // ✅ Safe fallback if table not found
    }

    public List<Records.DocumentAttribute> getReportAttributes(String documentName) {
        return this.dataService.getAttributesWithTypes(documentName);

    }

}
