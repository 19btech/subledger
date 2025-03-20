package com.fyntrac.common.key;

import java.util.Objects;

public class MetricLevelLtdKey implements AggregationLtdKey {
    protected final String metricName;
    protected final int accountingPeriodId;
    protected final String tenantId;

    public MetricLevelLtdKey(String tenantId, String metricName, int accountingPeriodId) {
        this.metricName = metricName;
        this.accountingPeriodId = accountingPeriodId;
        this.tenantId = tenantId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricLevelLtdKey that = (MetricLevelLtdKey) o;
        return accountingPeriodId == that.accountingPeriodId
                && Objects.equals(metricName, that.metricName)
                && Objects.equals(tenantId, that.tenantId);
    }

    private String getHashCode() {
        return  this.tenantId + Objects.hash(metricName, accountingPeriodId);
    }

    public String getKey() {
        return String.format("%s-%d-%s", tenantId, accountingPeriodId, metricName);
    }
}