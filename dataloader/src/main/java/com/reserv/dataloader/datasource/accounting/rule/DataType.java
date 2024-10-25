package  com.fyntrac.common.enums;

public enum DataType {
    STRING("String"),
    NUMBER("Number"),
    BOOLEAN("Boolean"),
    DATE("Date");

    private final String value;

    DataType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static boolean isValid(String text) {
        for (DataType rule : DataType.values()) {
            if (rule.value.equalsIgnoreCase(text)) {
                return true;
            }
        }
        return false; // If the input string doesn't match any enum value
    }
}
