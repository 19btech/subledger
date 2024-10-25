package  com.fyntrac.common.enums;

public enum AggregationLevel {
    ATTRIBUTE("Attribute"),
    INSTRUMENT("Instrument"),
    TENANT("Tenant");

    private final String value;

    AggregationLevel(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static boolean isValid(String text) {
        for (AggregationLevel rule : AggregationLevel.values()) {
            if (rule.value.equalsIgnoreCase(text)) {
                return true;
            }
        }
        return false; // If the input string doesn't match any enum value
    }
}
