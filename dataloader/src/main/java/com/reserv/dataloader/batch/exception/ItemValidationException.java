package com.reserv.dataloader.batch.exception;

import java.util.List;

public class ItemValidationException extends RuntimeException {
    
    public static class ValidationError {
        private final String column;
        private final String value;
        private final String errorCode;
        private final String message;
        private final String severity;

        public ValidationError(String column, String value, String errorCode, String message, String severity) {
            this.column = column;
            this.value = value;
            this.errorCode = errorCode;
            this.message = message;
            this.severity = severity;
        }

        public String getColumn() { return column; }
        public String getValue() { return value; }
        public String getErrorCode() { return errorCode; }
        public String getMessage() { return message; }
        public String getSeverity() { return severity; }
    }

    private final List<ValidationError> validationErrors;

    public ItemValidationException(String message, List<ValidationError> validationErrors) {
        super(message);
        this.validationErrors = validationErrors;
    }

    public List<ValidationError> getValidationErrors() {
        return validationErrors;
    }
}
