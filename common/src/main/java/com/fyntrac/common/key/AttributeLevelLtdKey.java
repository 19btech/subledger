package com.fyntrac.common.key;

import java.util.Objects;


public class AttributeLevelLtdKey extends InstrumentLevelLtdKey {
    protected final String attributeid;

    public AttributeLevelLtdKey(String tenantId, String metricName, String instrumentId, String attributeid, int accountingPeriodId) {
        super(tenantId, metricName, instrumentId, accountingPeriodId);
        this.attributeid = attributeid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AttributeLevelLtdKey that = (AttributeLevelLtdKey) o;
        return accountingPeriodId == that.accountingPeriodId
                && Objects.equals(metricName, that.metricName)
                && Objects.equals(tenantId, that.tenantId)
                && Objects.equals(instrumentId, that.instrumentId)
                && Objects.equals(attributeid,that.attributeid);
    }

    private String getHashCode() {
        return  this.tenantId + Objects.hash(metricName, instrumentId, attributeid, accountingPeriodId);
    }

    @Override
    public String getKey(){
        return this.getHashCode();
    }
}
