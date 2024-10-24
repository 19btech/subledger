package com.reserv.dataloader.records;

public class Records {
    // Record definition for Accounting Period
    public record AccountingPeriodRecord(int periodId, String period, int fiscalPeriod, int year, int status) {
    }
}
