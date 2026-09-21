package com.fyntrac.common.service;

import com.fyntrac.common.entity.CustomTableColumn;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.enums.CustomTableType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
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

        createIndexes(mongoTemplate, tableDefinition);
    }

    // Custom tables are named/created dynamically at runtime, so — unlike every other
    // collection in the app — there's no Java entity for @Indexed/@CompoundIndex annotations to
    // live on. primaryKeys was already captured on every CustomTableDefinition (used only for
    // the reference-table metadata document, see createReferenceTableMetadata) but never actually
    // indexed anywhere; confirmed live that both Billing_Schedule (12,139 docs) and PSDLogs
    // (6,463 docs) had nothing but the default _id index. This does it generally, from the
    // definition's own declared metadata, at creation time — instead of the previous approach of
    // one-off hardcoding the specific field list a specific caller happened to query by (see
    // ExcelModelService.ensureCustomTableIndex, kept as a lazy fallback for tables that already
    // existed before this method started running, since this only fires for newly-created ones).
    //
    // Non-unique deliberately: a primary key column repeating across rows is normal here (e.g.
    // Billing_Schedule's primary key is just "attributeId", which legitimately recurs once per
    // posting date for the same instrument/attribute over the table's 12k+ rows) — this is a
    // lookup-speed index, not a uniqueness constraint, and enforcing uniqueness would reject
    // otherwise-valid ingestion.
    private void createIndexes(MongoTemplate mongoTemplate, CustomTableDefinition tableDefinition) {
        String collectionName = tableDefinition.getTableName();
        List<String> primaryKeys = tableDefinition.getPrimaryKeys();
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            return;
        }
        try {
            org.bson.Document indexKeys = new org.bson.Document();
            for (String column : primaryKeys) {
                if (column != null && !column.isBlank()) {
                    indexKeys.append(column, 1);
                }
            }
            if (indexKeys.isEmpty()) {
                return;
            }
            mongoTemplate.indexOps(collectionName).ensureIndex(
                    new CompoundIndexDefinition(indexKeys).named(collectionName + "_primarykeys_idx"));
        } catch (Exception e) {
            // Never block table creation over an index that can always be added/retried later —
            // same spirit as MongoIndexEnsurer's per-index try/catch.
            log.warn("Failed to ensure primary-key index on custom table {}: {}", collectionName, e.getMessage());
        }

        // Operational tables (not reference tables) are the ones ExcelModelService.
        // getValuesFromCustomTable() queries per-instrument, filtered on whichever of
        // instrumentId/attributeId/postingDate/effectiveDate the table actually has — see its own
        // ensureCustomTableIndex for the full story. Creating it here too means a freshly-created
        // operational table never hits that method's lazy first-query path at all.
        //
        // Not assumed present: CustomTableDefinition.columns is this table's own declared schema
        // (from CustomTableColumn.columnName), so only the ones actually declared go into the
        // index — an operational table with none of these four columns gets no index from this
        // block at all, rather than a compound index built partly out of fields that don't exist
        // in its documents.
        if (tableDefinition.getTableType() == CustomTableType.OPERATIONAL) {
            Set<String> declaredColumns = tableDefinition.getColumns() == null ? Set.of()
                    : tableDefinition.getColumns().stream()
                            .map(CustomTableColumn::getColumnName)
                            .filter(java.util.Objects::nonNull)
                            .map(c -> c.toLowerCase(Locale.ROOT))
                            .collect(Collectors.toSet());

            Index index = new Index();
            boolean any = false;
            if (declaredColumns.contains("instrumentid")) {
                index.on("instrumentId", Sort.Direction.ASC);
                any = true;
            }
            if (declaredColumns.contains("attributeid")) {
                index.on("attributeId", Sort.Direction.ASC);
                any = true;
            }
            if (declaredColumns.contains("postingdate")) {
                index.on("postingDate", Sort.Direction.ASC);
                any = true;
            }
            if (declaredColumns.contains("effectivedate")) {
                index.on("effectiveDate", Sort.Direction.DESC);
                any = true;
            }

            if (any) {
                try {
                    mongoTemplate.indexOps(collectionName).ensureIndex(
                            index.named(collectionName + "_instrument_attribute_postingdate_idx"));
                } catch (Exception e) {
                    log.warn("Failed to ensure instrument/attribute/postingDate index on custom table {}: {}",
                            collectionName, e.getMessage());
                }
            }
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
