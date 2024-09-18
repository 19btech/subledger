package com.reserv.dataloader.entity;

import com.reserv.dataloader.key.MetricLevelLtdKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "MetricLevelLtd")
public class MetricLevelLtd implements Serializable, BaseLevelLtd {
    @Serial
    private static final long serialVersionUID = 3393182226432882651L;
    @Id
    private String id;
    private String metricName;
    @Indexed(unique = false)
    private int accountingPeriodId;
    BaseLtd balance;

    public String getKey(String tenantId) {
        return new MetricLevelLtdKey(tenantId,
                this.getMetricName().toUpperCase(),
                this.getAccountingPeriodId()).getKey();
    }
}
