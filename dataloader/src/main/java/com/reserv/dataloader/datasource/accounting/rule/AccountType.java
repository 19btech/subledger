package  com.fyntrac.common.enums;

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
        for (AccountType rule : AccountType.values()) {
            if (rule.value.equalsIgnoreCase(text)) {
                return true;
            }
        }
        return false; // If the input string doesn't match any enum value
    }

    }
