package com.fyntrac.common.service;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.enums.CustomTableType;
import com.fyntrac.common.repository.CustomTableDefinitionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class CustomTableDefinitionService {

    private final CustomTableDefinitionRepository tableDefinitionRepository;
    private final CustomTableCreationStrategyFactory strategyFactory;

    public CustomTableDefinitionService(CustomTableDefinitionRepository tableDefinitionRepository,
                                        CustomTableCreationStrategyFactory strategyFactory) {
        this.tableDefinitionRepository = tableDefinitionRepository;
        this.strategyFactory = strategyFactory;

    }

    public Optional<CustomTableDefinition> findById(String id) {
        return this.tableDefinitionRepository.findById(id);
    }
    // ... existing CRUD methods ...

    /**
     * Creates physical table using polymorphic strategy
     */
    public void createPhysicalTable(CustomTableDefinition tableDefinition) {
        CustomTableCreationStrategy strategy = strategyFactory.getStrategy(tableDefinition);
        strategy.createPhysicalTable(tableDefinition);
    }

    /**
     * Creates physical table directly from request
     */
    public void createPhysicalTable(Records.CustomTableRequestRecord request) {
        CustomTableDefinition tableDefinition = convertToTableDefinition(request);
        createPhysicalTable(tableDefinition);
    }

    /**
     * Creates both table definition and physical table in one transaction
     */
    @Transactional
    public CustomTableDefinition createTableWithPhysicalTable(Records.CustomTableRequestRecord request) {
        validateTableCreationRequest(request);

        CustomTableDefinition tableDefinition = convertToTableDefinition(request);
        CustomTableDefinition savedDefinition = tableDefinitionRepository.save(tableDefinition);

        try {
            createPhysicalTable(savedDefinition);
            return savedDefinition;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create physical table: " + e.getMessage(), e);
        }
    }

    /**
     * Drops physical table using polymorphic strategy
     */
    public void dropPhysicalTable(String tableName) {
        CustomTableDefinition tableDefinition = tableDefinitionRepository.findByTableName(tableName)
                .orElseThrow(() -> new IllegalArgumentException("Table definition not found for: " + tableName));

        CustomTableCreationStrategy strategy = strategyFactory.getStrategy(tableDefinition);
        strategy.dropPhysicalTable(tableName);
    }

    /**
     * Checks if physical table exists using polymorphic strategy
     */
    public boolean physicalTableExists(String tableName) {
        CustomTableDefinition tableDefinition = tableDefinitionRepository.findByTableName(tableName)
                .orElseThrow(() -> new IllegalArgumentException("Table definition not found for: " + tableName));

        CustomTableCreationStrategy strategy = strategyFactory.getStrategy(tableDefinition);
        return strategy.tableExists(tableName);
    }

    /**
     * Gets table statistics using appropriate strategy
     */
    public Map<String, Object> getTableStats(String tableName) {
        CustomTableDefinition tableDefinition = tableDefinitionRepository.findByTableName(tableName)
                .orElseThrow(() -> new IllegalArgumentException("Table definition not found for: " + tableName));

        CustomTableCreationStrategy strategy = strategyFactory.getStrategy(tableDefinition);

        Map<String, Object> stats = new HashMap<>();
        stats.put("tableName", tableName);
        stats.put("tableType", tableDefinition.getTableType());
        stats.put("physicalTableExists", strategy.tableExists(tableName));

        // Add strategy-specific statistics
        if (strategy instanceof MongoCustomTableCreationStrategy mongoStrategy) {
            stats.putAll(getMongoTableStats(tableName));
        }

        return stats;
    }

    /**
     * Gets table statistics using appropriate strategy
     */
    public CustomTableDefinition getCustomTableDefinition(String tableName) {
        CustomTableDefinition tableDefinition = tableDefinitionRepository.findByTableName(tableName)
                .orElseThrow(() -> new IllegalArgumentException("Table definition not found for: " + tableName));


        return tableDefinition;
    }

    // Private helper methods
    private CustomTableDefinition convertToTableDefinition(Records.CustomTableRequestRecord  request) {
        CustomTableDefinition tableDefinition = new CustomTableDefinition();
        tableDefinition.setTableName(request.tableName());
        tableDefinition.setDescription(request.description());
        tableDefinition.setTableType(request.tableType());
        tableDefinition.setColumns(request.columns());
        tableDefinition.setPrimaryKeys(request.primaryKeys());
        tableDefinition.setReferenceColumn(request.referenceColumn());
        tableDefinition.setReferenceTable(request.referenceTable());
        return tableDefinition;
    }

    private void validateTableCreationRequest(Records.CustomTableRequestRecord  request) {
        if (tableDefinitionRepository.existsByTableName(request.tableName())) {
            throw new IllegalArgumentException("Table with name '" + request.tableName() + "' already exists");
        }

        // Check if physical table already exists
        try {
            CustomTableDefinition tempDefinition = convertToTableDefinition(request);
            CustomTableCreationStrategy strategy = strategyFactory.getStrategy(tempDefinition);
            if (strategy.tableExists(request.tableName())) {
                throw new IllegalArgumentException("Physical table '" + request.tableName() + "' already exists");
            }
        } catch (IllegalArgumentException e) {
            // Table definition doesn't exist, which is fine
        }

        validateColumnUniqueness(request);
    }

    private void validateColumnUniqueness(Records.CustomTableRequestRecord request) {
        long distinctColumnNames = request.columns().stream()
                .map(col -> col.getColumnName().toLowerCase())
                .distinct()
                .count();

        if (distinctColumnNames != request.columns().size()) {
            throw new IllegalArgumentException("Column names must be unique");
        }
    }

    private Map<String, Object> getMongoTableStats(String tableName) {
        Map<String, Object> stats = new HashMap<>();
        try {
            // This would be implemented with MongoDB specific statistics
            stats.put("storageEngine", "MongoDB");
            // Add more MongoDB-specific stats as needed
        } catch (Exception e) {
            stats.put("error", "Could not retrieve MongoDB statistics");
        }
        return stats;
    }

    public Optional<List<CustomTableDefinition>> getCustomTables(CustomTableType customTableType) {
        return this.tableDefinitionRepository.findByTableType(customTableType);
    }
}