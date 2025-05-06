package com.fyntrac.common.entity;

import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.Objects;

@Document(collection = "InstrumentActivityState")
@CompoundIndex(def = "{'instrumentId': 1, 'attributeId': 1, 'maxTransactionDate': 1}", name = "Instrument_Activity_State_Index")
public class InstrumentActivityState {
    private String instrumentId;
    private String attributeId;
    private Integer maxTransactionDate; // Adjust type as needed

    // Getters and Setters


    @Override
    public String toString() {
        return "InstrumentActivityState{" +
                "instrumentId='" + instrumentId + '\'' +
                ", attributeId='" + attributeId + '\'' +
                ", maxTransactionDate=" + maxTransactionDate +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(instrumentId, attributeId, "InstrumentActivityState");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InstrumentActivityState that)) return false;
        return Objects.equals(instrumentId, that.instrumentId) &&
                Objects.equals(attributeId, that.attributeId) &&
                Objects.equals(maxTransactionDate, that.maxTransactionDate);
    }
}