package com.fyntrac.common.entity;

import com.fyntrac.common.key.MetricLevelLtdKey;
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
@Document(collection = "MetricLevelLtd")
// Covers MetricLevelLtdFlatteningWriter.buildQuery/buildPreviousQuery — the "find this record,
// or its most recent prior posting date, to seed the running balance" lookup done for every
// record the metric-level LTD job writes. No compound index existed here at all before.
// (findLatestByPostingDate(int) in MetricLevelAggregationService filters postingDate only, with
// no metricName, so the existing single-field postingDate index already covers that one.)
@CompoundIndex(def = "{'metricName': 1, 'postingDate': 1}", name = "MetricLevelLtd_metric_postingdate_index")
public class MetricLevelLtd implements Serializable, BaseLevelLtd {
    @Serial
    private static final long serialVersionUID = 3393182226432882651L;
    @Id
    private String id;
    @NotNull
    @Indexed
    private String metricName;
    @Indexed(unique = false)
    private int accountingPeriodId;
    @NotNull
    @Indexed
    private Integer postingDate;
    BaseLtd balance;

    public String getKey(String tenantId) {
        return new MetricLevelLtdKey(tenantId,
                this.getMetricName().toUpperCase(),
                this.getPostingDate()).getKey();
    }
}
