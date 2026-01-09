package com.fyntrac.common.enums;

public enum FileUploadActivityType {
    TRANSACTION("Transactions"),
    ATTRIBUTE("Attributes"),
    AGGREGATION("Aggregation"),
    ACCOUNT_TYPE("AccountType"),
    CHART_OF_ACCOUNT("ChartOfAccounts"),
    SUBLEDGER_MAPPING("SubledgerMapping"),
    TRANSACTION_ACTIVITY("TransactionActivity"),
    INSTRUMENT_ATTRIBUTE("InstrumentAttributeHistory"),
    ATTRIBUTE_BALANCE("AttributeBalance"),
    INSTRUMENT_BALANCE("InstrumentBalance"),
    CUSTOM_TABLE("CustomTable");

    private final String tableName;

    FileUploadActivityType(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
