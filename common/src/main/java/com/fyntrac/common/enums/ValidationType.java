package com.fyntrac.common.enums;

/**
 * Categorises validation log entries by the pipeline that produced them.
 *
 * <ul>
 *   <li>{@link #ACCOUNTING_RULES} – Transactions, Aggregation, Attributes validators</li>
 *   <li>{@link #JOURNAL_MAPPING}  – AccountTypes, ChartOfAccount, SubledgerMapping validators</li>
 *   <li>{@link #ACTIVITY}         – InstrumentAttribute, TransactionActivity validators</li>
 *   <li>{@link #CUSTOM_ACTIVITY}  – DynamicTable validator</li>
 * </ul>
 */
public enum ValidationType {
    ACCOUNTING_RULES,
    JOURNAL_MAPPING,
    ACTIVITY,
    CUSTOM_ACTIVITY
}
