package com.fyntrac.common.entity;

import com.fyntrac.common.key.AttributeLevelLtdKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "AttributeLevelLtd")
@CompoundIndex(def = "{'instrumentId': 1, 'attributeId': 1}", name = "AttributeLevelLtd_attribute_instrument_index")
public class AttributeLevelLtd implements Serializable, BaseLevelLtd {
    @Serial
    private static final long serialVersionUID = 4630237140330001617L;
    @Id
    private String id;
    @Indexed
    private String metricName;
    private String instrumentId;
    private String attributeId;
    @Indexed
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

