package com.fyntrac.common.enums;


public enum AccountType {
    BALANCESHEET("BalanceSheet"),
    INCOMESTATEMENT("IcomeStatement"),
    CLEARING("Clearing");

    private final String value;

    AccountType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static boolean isValid(String text) {
        // Remove all spaces from the input text
        String noSpaceText = text.replaceAll("\\s+", "");

        // Validate against AccountType enum values
        for (AccountType rule : AccountType.values()) {
            if (rule.value.equalsIgnoreCase(noSpaceText)) {
                return true;
            }
        }
        return false; // If the input string doesn't match any enum value
    }

    }
