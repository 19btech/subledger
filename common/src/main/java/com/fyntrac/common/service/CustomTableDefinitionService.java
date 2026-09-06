package com.fyntrac.common.service;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.enums.CustomTableType;
import com.fyntrac.common.repository.CustomTableDefinitionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
@Slf4j
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

    /**
     * Lists the still-active (non soft-deleted) tables of the given type. Backs the
     * reference-tables/operational-tables listing endpoints, so a soft-deleted definition drops
     * out of both grids without its physical collection or data ever being touched.
     */
    public Optional<List<CustomTableDefinition>> getCustomTables(CustomTableType customTableType) {
        return Optional.of(this.tableDefinitionRepository.findByTableTypeAndIsDeletedFalse(customTableType));
    }

    /**
     * Soft-deletes a custom table definition and cascades across a REFERENCE/OPERATIONAL pair
     * so the two never end up half-deleted relative to each other:
     * <ul>
     *   <li>Deleting a REFERENCE table also soft-deletes every still-active OPERATIONAL table
     *       that points at it via referenceTable - that operational data has no meaningful
     *       lookup table left otherwise.</li>
     *   <li>Deleting an OPERATIONAL table also soft-deletes the REFERENCE table it points at,
     *       but only if no other still-active OPERATIONAL table depends on that same reference
     *       table - it may still be needed elsewhere.</li>
     * </ul>
     * This never touches the underlying physical Mongo collections or their data - only the
     * table definitions are flagged, so the delete is fully reversible by clearing isDeleted.
     *
     * @param id the database id of the table definition to delete.
     * @return the ids of every table definition that was soft-deleted as a result (the
     *         requested one plus any cascaded), in the order they were deleted.
     */
    @Transactional
    public List<String> softDeleteById(String id) {
        CustomTableDefinition table = tableDefinitionRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Table definition not found for id: " + id));

        List<String> deletedIds = new ArrayList<>();
        if (tableDefinitionRepository.softDeleteById(id) > 0) {
            deletedIds.add(id);
        }

        if (table.getTableType() == CustomTableType.REFERENCE) {
            List<CustomTableDefinition> dependents = tableDefinitionRepository
                    .findByTableTypeAndReferenceTableAndIsDeletedFalse(CustomTableType.OPERATIONAL, table.getTableName());
            for (CustomTableDefinition dependent : dependents) {
                if (tableDefinitionRepository.softDeleteById(dependent.getId()) > 0) {
                    deletedIds.add(dependent.getId());
                    log.info("Cascaded soft-delete: OPERATIONAL table [{}] soft-deleted with its REFERENCE table [{}].",
                            dependent.getTableName(), table.getTableName());
                }
            }
        } else if (table.getTableType() == CustomTableType.OPERATIONAL
                && table.getReferenceTable() != null && !table.getReferenceTable().isBlank()) {
            List<CustomTableDefinition> remainingDependents = tableDefinitionRepository
                    .findByTableTypeAndReferenceTableAndIsDeletedFalse(CustomTableType.OPERATIONAL, table.getReferenceTable());
            if (remainingDependents.isEmpty()) {
                tableDefinitionRepository.findByTableName(table.getReferenceTable())
                        .filter(ref -> !ref.isDeleted())
                        .ifPresent(ref -> {
                            if (tableDefinitionRepository.softDeleteById(ref.getId()) > 0) {
                                deletedIds.add(ref.getId());
                                log.info("Cascaded soft-delete: REFERENCE table [{}] soft-deleted - no remaining " +
                                        "OPERATIONAL table depends on it after [{}] was deleted.",
                                        ref.getTableName(), table.getTableName());
                            }
                        });
            }
        }

        return deletedIds;
    }

}