package com.fyntrac.common.entity;

import com.fyntrac.common.key.InstrumentLevelLtdKey;
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
@Document(collection = "InstrumentLevelLtd")
// Covers InstrumentLevelLtdFlatteningWriter.buildQuery/buildPreviousQuery — the "find this
// record, or its most recent prior posting date, to seed the running balance" lookup done for
// every record the instrument-level LTD job writes. No compound index existed here at all before.
@CompoundIndex(def = "{'metricName': 1, 'instrumentId': 1, 'postingDate': 1}",
        name = "InstrumentLevelLtd_metric_instrument_postingdate_index")
public class InstrumentLevelLtd implements Serializable, BaseLevelLtd {
    @Serial
    private static final long serialVersionUID = -7398942930729219386L;
    @Id
    private String id;
    @NotNull
    @Indexed
    private String metricName;
    @NotNull
    @Indexed
    private String instrumentId;
    @Indexed(unique = false)
    private int accountingPeriodId;
    @NotNull
    @Indexed
    private Integer postingDate;
    BaseLtd balance;

    public String getKey(String tenantId) {
        return new InstrumentLevelLtdKey(tenantId,
                this.getMetricName().toUpperCase(),
                this.getInstrumentId(),
                this.getPostingDate()).getKey();
    }
}
