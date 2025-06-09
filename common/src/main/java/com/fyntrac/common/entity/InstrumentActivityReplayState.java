package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Objects;


@Data
@Builder
@AllArgsConstructor
@Document(collection = "InstrumentActivityReplayState")
@CompoundIndex(def = "{'instrumentId': 1, 'attributeId': 1, 'minTransactionDate': 1}", name = "Instrument_Activity_Replay_State_Index")
public class InstrumentActivityReplayState {
    private String instrumentId;
    private String attributeId;
    private Integer minTransactionDate; // Adjust type as needed

    // Getters and Setters


    @Override
    public String toString() {
        return "InstrumentActivityState{" +
                "instrumentId='" + instrumentId + '\'' +
                ", attributeId='" + attributeId + '\'' +
                ", minTransactionDate=" + minTransactionDate +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(instrumentId, attributeId, "InstrumentActivityReplayState");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InstrumentActivityReplayState that)) return false;
        return Objects.equals(instrumentId, that.instrumentId) &&
                Objects.equals(attributeId, that.attributeId) &&
                Objects.equals(minTransactionDate, that.minTransactionDate);
    }
}