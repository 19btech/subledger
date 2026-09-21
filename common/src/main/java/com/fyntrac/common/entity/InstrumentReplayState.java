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
// Was declared as {..., 'minTransactionDate': 1} — that field doesn't exist on this entity (the
// actual field, and what InstrumentReplayStateService.getInstrumentAttributeReplayState() and
// getInstrumentReplayState() actually query on, is maxPostingDate) — so this index was never
// usable by either real query. Corrected to match.
// Renamed rather than kept as "Instrument_Activity_Replay_State_Index": any database that
// already had the old (broken) index physically created under that name rejects redefining the
// same name with different keys (MongoDB IndexOptionsConflict) — ensureIndex isn't a safe way to
// change an existing index's keys in place. A new name just creates a new index instead; the old,
// unused one is harmless and can be dropped manually later if desired.
@CompoundIndex(def = "{'instrumentId': 1, 'attributeId': 1, 'maxPostingDate': 1}", name = "InstrumentReplayState_instrument_attribute_maxpostingdate_index")
public class InstrumentReplayState implements Serializable {
    @Serial
    private static final long serialVersionUID = -4134702344331969798L;
    @Id
    private String id;
    private String instrumentId;
    private String attributeId;
    private Integer minEffectiveDate;
    private Integer maxPostingDate;//


    // Getters and Setters


    @Override
    public String toString() {
        return "InstrumentReplayState{" +
                "instrumentId:'" + instrumentId + '\'' +
                "attributeId:'" + attributeId + '\'' +
                ", minTransactionDate:" + minEffectiveDate +
                ", maxPostingDate:" + maxPostingDate +
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