package com.fyntrac.common.enums;

public enum ErrorCode {
    Model_Execution_Error("Model Execution Error"),
    ERR_BOOL_01("INVALID_BOOLEAN"),
    WRN_DEF_01("MISSING_FLAG_DEFAULT"),
    WRN_LOGIC_01("NULL_ACTION_WARNING"),
    ERR_REQ_01("MANDATORY_FIELD"),
    ERR_SPC_01("NO_WHITESPACE"),
    ERR_SPC_02("TRIM_WHITESPACE"),
    ERR_FMT_01("ALPHANUM_UNDERSCORE"),
    ERR_DUP_01("DUPLICATE_VALUE"),
    ERR_REF_01("REF_NOT_FOUND"),
    ERR_LIST_01("INVALID_ALLOWED_VAL"),
    ERR_LIST_02("INVALID_YES_NO"),
    ERR_LOGIC_02("LOGIC_DEPENDENCY"),
    ERR_LOGIC_03("LOGIC_MULTI_MAP"),
    ERR_REF_02("REF_CONFIG_MISSING"),
    ERR_LOGIC_04("LOGIC_AMBIGUOUS"),
    ERR_DUP_02("DUPLICATE_RULE"),
    ERR_LOGIC_05("LOGIC_ENTRY_CONFLICT"),
    ERR_LOGIC_06("LOGIC_SUBTYPE_CLASH"),
    ERR_TYPE_01("INVALID_DATATYPE"),
    ERR_SPC_03("COL_VAL_WHITESPACE"),
    ERR_REQ_02("MANDATORY_DATE"),
    ERR_FMT_DT("INVALID_DATE_FORMAT"),
    ERR_REQ_03("MANDATORY_ID"),
    ERR_REF_03("REF_INSTRUMENT_MISSING"),
    ERR_SPC_04("ID_WHITESPACE"),
    ERR_REQ_04("MANDATORY_AMOUNT"),
    ERR_FMT_DEC("INVALID_DECIMAL_FMT"),
    ERR_REF_04("REF_ATTR_MISSING"),
    ERR_REQ_05("MANDATORY_NULL_CHECK"),
    ERR_DUP_03("DUPLICATE_TABLE"),
    ERR_FMT_02("FORMAT_STRING_US"),
    ERR_DUP_04("DUPLICATE_COLUMN"),
    ERR_REF_05("REF_CUSTOM_MISSING"),
    ERR_REF_06("REF_ATTR_MISSING"),
    ERR_REF_08("REF_INSTRUMENT_NOT_FOUND"),
    ERR_REF_09("REF_ATTRIBUTE_NOT_FOUND");

    private final String value;

    ErrorCode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public String getCode() {
        return name();
    }

    public String getName() {
        return value;
    }

    public static boolean isValid(String text) {
        for (ErrorCode errorCode : ErrorCode.values()) {
            if (errorCode.value.equalsIgnoreCase(text)) {
                return true;
            }
        }
        return false;
    }
}
