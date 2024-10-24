package com.fyntrac.common.enums;

public enum AccountingRules {
    TRANSACIONS("transactions.csv"),
    ATTRIBUTES("attributes.csv"),
    AGGREGATION("aggregation.csv"),
    ACCOUNTTYPE("accounttype.csv"),
    CHARTOFACCOUNTS("chartofaccounts.csv"),
    ACCOUNTMAPPINGS("accountmappings.csv"),
    CHARTOFACCOUNT("chartofaccount.csv"),
    SUBLEDGERMAPPING("subledgermapping.csv"),
    INSTRUMENTATTRIBUTE("instrumentattribute.csv"),
    TRANSACTIONACTIVITY("transactionactivity.csv");

    private final String value;

    AccountingRules(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    // Method to check if the input string matches any value in the AccountingRules enum
    public static boolean isValid(String text) {
        for (AccountingRules rule : AccountingRules.values()) {
            if (rule.value.equalsIgnoreCase(text)) {
                return true;
            }
        }
        return false; // If the input string doesn't match any enum value
    }

    public static AccountingRules get(String text) {
        for (AccountingRules rule : AccountingRules.values()) {
            if (rule.value.equalsIgnoreCase(text)) {
                return rule;
            }
        }
        return null; // If the input string doesn't match any enum value
    }

    // Factory method to return the appropriate file uploader based on the enum value
}