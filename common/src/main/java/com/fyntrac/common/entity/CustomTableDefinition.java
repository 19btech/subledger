package com.fyntrac.common.entity;

import com.fyntrac.common.enums.CustomTableType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import jakarta.validation.Valid;
import lombok.Getter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.index.Indexed;

import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;

@Getter
@Document(collection = "CustomTableDefinitions")
public class CustomTableDefinition {

    // Getters and Setters
    @Id
    private String id;

    @NotBlank(message = "Table name is required")
    @Pattern(regexp = "^[a-zA-Z_][a-zA-Z0-9_]*$",
            message = "Table name must start with a letter or underscore and contain only letters, numbers, and underscores")
    @Size(max = 63, message = "Table name cannot exceed 63 characters")
    @Indexed(unique = true)
    private String tableName;

    private String description;

    @NotNull(message = "Table type is required")
    private CustomTableType tableType;

    @NotNull(message = "Columns are required")
    @Valid
    private List<CustomTableColumn> columns = new ArrayList<>();

    @NotNull(message = "Primary keys are required")
    private List<String> primaryKeys = new ArrayList<>();

    private String referenceColumn;

    private String referenceTable;

    private final LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    // Constructors
    public CustomTableDefinition() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    public CustomTableDefinition(String tableName, String description, CustomTableType tableType) {
        this();
        this.tableName = tableName;
        this.description = description;
        this.tableType = tableType;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
        this.updatedAt = LocalDateTime.now();
    }

    public void setDescription(String description) {
        this.description = description;
        this.updatedAt = LocalDateTime.now();
    }

    public void setTableType(CustomTableType tableType) {
        this.tableType = tableType;
        this.updatedAt = LocalDateTime.now();
    }

    public void setColumns(List<CustomTableColumn> columns) {
        this.columns = columns;
        this.updatedAt = LocalDateTime.now();
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        this.updatedAt = LocalDateTime.now();
    }

    public void setReferenceColumn(String referenceColumn) {
        this.referenceColumn = referenceColumn;
        this.updatedAt = LocalDateTime.now();
    }

    public void setReferenceTable(String referenceTable) {
        this.referenceTable = referenceTable;
        this.updatedAt = LocalDateTime.now();
    }

    public void setId(String id) {
        this.id = id;
    }

    // Helper methods
    public void addColumn(CustomTableColumn column) {
        this.columns.add(column);
        this.updatedAt = LocalDateTime.now();
    }

    public void removeColumn(CustomTableColumn column) {
        this.columns.remove(column);
        this.updatedAt = LocalDateTime.now();
    }

    public void addPrimaryKey(String primaryKey) {
        this.primaryKeys.add(primaryKey);
        this.updatedAt = LocalDateTime.now();
    }

    public void removePrimaryKey(String primaryKey) {
        this.primaryKeys.remove(primaryKey);
        this.updatedAt = LocalDateTime.now();
    }

    // Validation helper methods
    public boolean hasDuplicateColumnNames() {
        if (columns == null || columns.isEmpty()) return false;

        long distinctCount = columns.stream()
                .map(CustomTableColumn::getColumnName)
                .map(String::toLowerCase)
                .distinct()
                .count();

        return distinctCount != columns.size();
    }

    public boolean isValidReferenceColumn() {
        if (tableType != CustomTableType.REFERENCE) return true;
        if (referenceColumn == null || referenceColumn.trim().isEmpty()) return false;

        return columns.stream()
                .anyMatch(col -> referenceColumn.equals(col.getColumnName()));
    }

}