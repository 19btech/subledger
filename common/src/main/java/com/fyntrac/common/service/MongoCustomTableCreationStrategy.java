package com.fyntrac.common.service;

import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.enums.CustomTableType;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class MongoCustomTableCreationStrategy implements CustomTableCreationStrategy {

    private final DataService dataService;

    public MongoCustomTableCreationStrategy(DataService dataService) {
        this.dataService = dataService;
    }

    @Override
    public boolean supports(CustomTableDefinition tableDefinition) {
        // This strategy supports all table types for MongoDB
        return true;
    }

    @Override
    public void createPhysicalTable(CustomTableDefinition tableDefinition) {
        String collectionName = tableDefinition.getTableName();
        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate();
        if (mongoTemplate.collectionExists(collectionName)) {
            throw new IllegalArgumentException("Collection '" + collectionName + "' already exists");
        }

        // Create the collection
        mongoTemplate.createCollection(collectionName);

        // For reference tables, create an initial metadata document
        if (tableDefinition.getTableType() == CustomTableType.REFERENCE) {
            createReferenceTableMetadata(tableDefinition);
        }


    }

    @Override
    public void dropPhysicalTable(String tableName) {
        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate();
        if (mongoTemplate.collectionExists(tableName)) {
            mongoTemplate.dropCollection(tableName);
        }
    }

    @Override
    public boolean tableExists(String tableName) {
        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate();
        return mongoTemplate.collectionExists(tableName);
    }

    private void createReferenceTableMetadata(CustomTableDefinition tableDefinition) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_type", "reference_table_metadata");
        metadata.put("tableName", tableDefinition.getTableName());
        metadata.put("referenceColumn", tableDefinition.getReferenceColumn());
        metadata.put("description", tableDefinition.getDescription());
        metadata.put("columns", tableDefinition.getColumns());
        metadata.put("primaryKeys", tableDefinition.getPrimaryKeys());
        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate();
        mongoTemplate.insert(metadata, "table_metadata");
    }

}
