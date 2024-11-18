package com.fyntrac.common.enums;

public enum AccountingRules {
    TRANSACIONS("transactions.csv",100),
    ATTRIBUTES("attributes.csv",90),
    AGGREGATION("aggregation.csv",80),
    ACCOUNTTYPE("accounttype.csv",70),
    CHARTOFACCOUNTS("chartofaccounts.csv", 60),
    ACCOUNTMAPPINGS("accountmappings.csv",50),
    CHARTOFACCOUNT("chartofaccount.csv",40),
    SUBLEDGERMAPPING("subledgermapping.csv",30),
    INSTRUMENTATTRIBUTE("instrumentattribute.csv",20),
    TRANSACTIONACTIVITY("transactionactivity.csv",10);

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