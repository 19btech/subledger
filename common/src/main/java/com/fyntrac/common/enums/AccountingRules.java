package com.fyntrac.common.enums;

public enum AccountingRules {
    TRANSACIONS("transactions.csv",100),
    ATTRIBUTES("attributes.csv",95),
    AGGREGATION("aggregation.csv",90),
    ACCOUNTTYPE("accounttype.csv",85),
    CHARTOFACCOUNTS("chartofaccounts.csv", 80),
    ACCOUNTMAPPINGS("accountmappings.csv",75),
    CHARTOFACCOUNT("chartofaccount.csv",70),
    SUBLEDGERMAPPING("subledgermapping.csv",65),
    INSTRUMENTATTRIBUTE("instrumentattribute.csv",60),
    TRANSACTIONACTIVITY("transactionactivity.csv",55),
    ATTRIBUTEBALANCE("attributebalance.csv",50),
    INSTRUMENTBALANCE("instrumentbalance.csv",45);

    private final String value;
    private final int priority;

    AccountingRules(String value, int priority) {
        this.value = value;
        this.priority = priority;
    }

    public String getValue() {
        return value;
    }

    public int getPriority() {
        return priority;
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