package com.fyntrac.common.enums;

public enum FieldType {
    AGGREGATED("Aggregated"),
    ARRAY("Array"),
    NONE("None");

    private final String displayName;

    FieldType(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public static FieldType fromDisplayName(String displayName) {
        for (FieldType table : values()) {
            if (table.displayName.equals(displayName)) {
                return table;
            }
        }
        throw new IllegalArgumentException("Unknown FieldType " + displayName);
    }
}
