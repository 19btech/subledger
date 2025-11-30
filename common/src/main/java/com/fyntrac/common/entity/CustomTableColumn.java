package com.fyntrac.common.entity;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.data.annotation.Transient;
import java.util.Arrays;
import java.util.List;

public class CustomTableColumn {

    @NotBlank(message = "User field is required")
    private String userField;

    @NotBlank(message = "Column name is required")
    @Pattern(regexp = "^[a-zA-Z_][a-zA-Z0-9_]*$",
            message = "Column name must start with a letter or underscore and contain only letters, numbers, and underscores")
    @Size(max = 63, message = "Column name cannot exceed 63 characters")
    private String columnName;

    @NotNull(message = "Data type is required")
    private DataType dataType;

    @NotNull(message = "Nullable flag is required")
    private Boolean nullable;

    @NotNull(message = "Display order is required")
    private Integer displayOrder;

    // Constructors
    public CustomTableColumn() {}

    public CustomTableColumn(String userField, String columnName, DataType dataType, Boolean nullable, Integer displayOrder) {
        this.userField = userField;
        this.columnName = columnName;
        this.dataType = dataType;
        this.nullable = nullable;
        this.displayOrder = displayOrder;
    }

    // Getters and Setters
    public String getUserField() { return userField; }
    public void setUserField(String userField) { this.userField = userField; }

    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }

    public DataType getDataType() { return dataType; }
    public void setDataType(DataType dataType) { this.dataType = dataType; }

    public Boolean getNullable() { return nullable; }
    public void setNullable(Boolean nullable) { this.nullable = nullable; }

    public Integer getDisplayOrder() { return displayOrder; }
    public void setDisplayOrder(Integer displayOrder) { this.displayOrder = displayOrder; }

    public enum DataType {
        STRING, NUMBER, DATE, BOOLEAN
    }

    @Transient
    private static final List<String> RESERVED_KEYWORDS = Arrays.asList(
            "select", "insert", "update", "delete", "create", "drop", "table",
            "column", "key", "from", "where", "join", "and", "or", "not",
            "null", "primary", "foreign", "references"
    );

    public static boolean isReservedKeyword(String columnName) {
        if (columnName == null) return false;
        return RESERVED_KEYWORDS.contains(columnName.toLowerCase());
    }

    public static String validateColumnName(String columnName) {
        if (columnName == null || columnName.trim().isEmpty()) {
            return "Column name is required";
        }

        // Must start with a letter or underscore, then letters/numbers/underscores
        String regex = "^[a-zA-Z_][a-zA-Z0-9_]*$";
        if (!columnName.matches(regex)) {
            return "Invalid column name: start with letter/underscore, use letters/numbers/underscores only";
        }

        if (columnName.length() > 63) {
            return "Column name cannot exceed 63 characters";
        }

        if (isReservedKeyword(columnName)) {
            return "Cannot use SQL reserved keyword";
        }

        return null;
    }
}