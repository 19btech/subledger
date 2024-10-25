package  com.fyntrac.common.enums;

public enum Sign {
    POSITIVE("Positive"),
    NEGATIVE("Negative");

    private final String value;

    Sign(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static boolean isValid(String text) {
        for (Sign sign : Sign.values()) {
            if (sign.value.equalsIgnoreCase(text)) {
                return true;
            }
        }
        return false; // If the input string doesn't match any enum value
    }
}
