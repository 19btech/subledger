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

import javax.validation.constraints.NotNull;
import java.io.Serial;
import java.io.Serializable;
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "AttributeLevelLtd")
@CompoundIndex(def = "{'instrumentId': 1, 'attributeId': 1}", name = "AttributeLevelLtd_attribute_instrument_index")
// Covers AttributeLevelLtdFlatteningWriter.buildQuery/buildPreviousQuery and
// AttributeLevelBalanceRepository.findLatestByPostingDate — all filter/sort on exactly these
// four fields (the "find this record, or its most recent prior posting date, to seed the running
// balance" lookup done for every record the attribute-level LTD job writes).
@CompoundIndex(def = "{'metricName': 1, 'instrumentId': 1, 'attributeId': 1, 'postingDate': 1}",
        name = "AttributeLevelLtd_metric_instrument_attribute_postingdate_index")
// Covers AttributeLevelBalanceRepository.findLatestBalanceByMetrics — the ON_MODEL_EXECUTION
// "balances" source-mapping path, run once per instrument. Its $match is {instrumentId,
// attributeId, postingDate: $lte, metricName: $in}, so unlike the metricName-first index above,
// this one needs instrumentId/attributeId leading (the plain equality filters) with postingDate
// third (matches the $lte range and the $sort on it) — unindexed, this fell back to the old
// {instrumentId, attributeId} prefix alone and then scanned every posting date on record for
// that instrument/attribute, which is exactly the same "gets slower as more dates accumulate"
// shape already fixed once for TransactionActivity.
@CompoundIndex(def = "{'instrumentId': 1, 'attributeId': 1, 'postingDate': 1}",
        name = "AttributeLevelLtd_instrument_attribute_postingdate_index")
public class AttributeLevelLtd implements Serializable, BaseLevelLtd {
    @Serial
    private static final long serialVersionUID = 4630237140330001617L;
    @Id
    private String id;
    @NotNull
    @Indexed
    private String metricName;
    @NotNull
    @Indexed
    private String instrumentId;
    @NotNull
    @Indexed
    private String attributeId;
    @Indexed
    private int accountingPeriodId;
    @NotNull
    @Indexed
    private Integer postingDate;
    BaseLtd balance;

    public void setMetricName() {
        setMetricName(null);
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }
    public String getKey(String tenantId) {
        return new AttributeLevelLtdKey(tenantId,
                this.getMetricName().toUpperCase(),
                this.getInstrumentId(),
                this.getAttributeId(),
                this.getPostingDate()).getKey();
    }
}

