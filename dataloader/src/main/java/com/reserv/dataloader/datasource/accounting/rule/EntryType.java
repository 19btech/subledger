package com.reserv.dataloader.datasource.accounting.rule;

public enum EntryType {
    DEBIT("Debit"),
    CREDIT("Credit");

    private final String value;

    EntryType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static boolean isValid(String text) {
        for (EntryType entryType : EntryType.values()) {
            if (entryType.value.equalsIgnoreCase(text)) {
                return true;
            }
        }
        return false; // If the input string doesn't match any enum value
    }
}

