package com.fyntrac.common.enums;

public enum TriggerType {
    ON_MODEL_EXECUTION("On Model Execution"),
    ON_ATTRIBUTE_ADD("On Attribute Add"),
    ON_TRANSACTION_POST("On Transaction Post"),
    ON_CONDITION_MATCH("On Condition Match"),
    ON_ATTRIBUTE_CHANGE("On Attribute Change");

    private final String displayName;

    TriggerType(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public static TriggerType fromDisplayName(String displayName) {
        for (TriggerType type : values()) {
            if (type.displayName.equals(displayName)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown trigger type: " + displayName);
    }
}