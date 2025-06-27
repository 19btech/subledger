package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

@Data
@Builder
@AllArgsConstructor
@Document(collection = "InstrumentReplayState")
@CompoundIndex(def = "{'instrumentId': 1, 'attributeId': 1, 'minTransactionDate': 1}", name = "Instrument_Activity_Replay_State_Index")
public class InstrumentReplayState implements Serializable {
    @Serial
    private static final long serialVersionUID = -4134702344331969798L;
    @Id
    private String id;
    private String instrumentId;
    private Integer minEffectiveDate;
    private Integer maxPostingDate;//


    // Getters and Setters


    @Override
    public String toString() {
        return "InstrumentReplayState{" +
                "instrumentId='" + instrumentId + '\'' +
                ", minTransactionDate=" + minEffectiveDate +
                ", maxPostingDate=" + maxPostingDate +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(instrumentId, "InstrumentReplayState");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InstrumentReplayState that)) return false;
        return Objects.equals(instrumentId, that.instrumentId) &&
                Objects.equals(minEffectiveDate, that.minEffectiveDate) &&
                Objects.equals(maxPostingDate, that.maxPostingDate);
    }
}