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
    /**
     * Generates a unique cache key based on tenant ID, accounting period, instrument, attribute, and metric.
     *
     * @param tenantId           The tenant ID.
     * @param accountingPeriodId The accounting period ID.
     * @param instrumentId       The instrument ID.
     * @param metricName         The metric name.
     * @return A unique cache key for the given parameters.
     */
    public String getKey() {
        return String.format("%s-%d-%s-%s-%s", tenantId, accountingPeriodId, instrumentId, attributeid, metricName);
    }
}
