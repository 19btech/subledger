package com.fyntrac.common.enums;

public enum SourceTable {
    ATTRIBUTE("Attribute"),
    TRANSACTIONS("Transactions"),
    BALANCES("Balances"),
    EXECUTIONSTATE("ExecutionState");

    private final String displayName;

    SourceTable(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public static SourceTable fromDisplayName(String displayName) {
        for (SourceTable table : values()) {
            if (table.displayName.equals(displayName)) {
                return table;
            }
        }
        throw new IllegalArgumentException("Unknown source table: " + displayName);
    }
}
