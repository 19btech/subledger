package com.reserv.dataloader.entity;

import com.reserv.dataloader.key.AttributeLevelLtdKey;
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
@Document(collection = "AttributeLevelLtd")

public class AttributeLevelLtd implements Serializable, BaseLevelLtd {
    @Serial
    private static final long serialVersionUID = 4630237140330001617L;
    @Id
    private String id;
    private String metricName;
    private String instrumentId;
    private String attributeId;
    @Indexed(unique = false)
    private int accountingPeriodId;
    BaseLtd balance;

    public String getKey(String tenantId) {
        return new AttributeLevelLtdKey(tenantId,
                this.getMetricName().toUpperCase(),
                this.getInstrumentId(),
                this.getAttributeId(),
                this.getAccountingPeriodId()).getKey();
    }
}

