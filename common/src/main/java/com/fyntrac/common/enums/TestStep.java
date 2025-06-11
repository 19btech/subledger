package com.fyntrac.common.enums;

public enum TestStep {
    LOAD_REF_DATA,
    ACTIVITY_UPLOAD,
    MODEL_UPLOAD,
    MODEL_CONFIGURATION,
    MODEL_EXECUTION;

    public static TestStep step(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Step value cannot be null or blank");
        }
        return TestStep.valueOf(value.trim().toUpperCase());
    }
}
