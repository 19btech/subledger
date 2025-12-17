package com.fyntrac.common.enums;

public enum TestStep {
    LOAD_REF_DATA,
    ACTIVITY_UPLOAD,
    MODEL_UPLOAD,
    MODEL_CONFIGURATION,
    MODEL_EXECUTION,
    EVENT_CONFIGURATION,
    CUSTOM_TABLE_DEFINITION,
    UPLOAD_CUSTOM_DATA;

    public static TestStep step(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Step value cannot be null or blank");
        }
        return TestStep.valueOf(value.trim().toUpperCase());
    }
}
