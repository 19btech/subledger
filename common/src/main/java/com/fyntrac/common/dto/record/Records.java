package com.fyntrac.common.dto.record;

import java.util.Date;

public class Records {
    // Record definition for Accounting Period
    public record AccountingPeriodRecord(int periodId, String period, int fiscalPeriod, int year, int status) {
    }

    public record GeneralLedgerMessageRecord(String tenantId, String dataKey){}
    public record TransactionActivityRecord(
            String tenantId,
            String id,
            Date transactionDate,
            String instrumentId,
            String transactionName,
            double value,
            String attributeId,
            int periodId,
            int originalPeriodId
    ) {
        // No additional methods are needed unless you want to add custom behavior
    }
}