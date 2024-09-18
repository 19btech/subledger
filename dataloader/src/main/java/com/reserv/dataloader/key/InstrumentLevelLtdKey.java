package com.reserv.dataloader.key;

import java.util.Objects;

public class InstrumentLevelLtdKey extends MetricLevelLtdKey{
    protected final String instrumentId;

    public InstrumentLevelLtdKey(String tenantId, String metricName, String instrumentId, int accountingPeriodId) {
        super(tenantId, metricName, accountingPeriodId);
        this.instrumentId = instrumentId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InstrumentLevelLtdKey that = (InstrumentLevelLtdKey) o;
        return accountingPeriodId == that.accountingPeriodId
                && Objects.equals(metricName, that.metricName)
                && Objects.equals(tenantId, that.tenantId)
                && Objects.equals(instrumentId, that.instrumentId);
    }

    private String getHashCode() {
        return  this.tenantId + Objects.hash(metricName, instrumentId, accountingPeriodId);
    }

    @Override
    public String getKey() {
        return this.getHashCode();
    }
}
