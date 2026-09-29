package com.fyntrac.common.entity;

import com.fyntrac.common.enums.EntryType;
import com.fyntrac.common.enums.Sign;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "SubledgerMapping")
public class SubledgerMapping implements Cloneable, Serializable {
    @Serial
    private static final long serialVersionUID= -199377848388208678L;
    @Id
    private String id;
    private String transactionName;
    private Sign sign;
    private EntryType entryType;
    private String accountSubType;
    private boolean isDeleted;

    // The SIGN / ENTRYTYPE cells exactly as uploaded, carried from the upload reader to its validator
    // on the row itself, so an invalid value can be reported verbatim. Not persisted (@Transient), not
    // Java-serialized or compared (transient), and without bean accessors so it stays out of JSON.
    @org.springframework.data.annotation.Transient
    @lombok.Getter(lombok.AccessLevel.NONE)
    @lombok.Setter(lombok.AccessLevel.NONE)
    private transient String rawSign;
    @org.springframework.data.annotation.Transient
    @lombok.Getter(lombok.AccessLevel.NONE)
    @lombok.Setter(lombok.AccessLevel.NONE)
    private transient String rawEntryType;

    public String rawSign() {
        return rawSign;
    }

    public String rawEntryType() {
        return rawEntryType;
    }

    public void rawValues(String rawSign, String rawEntryType) {
        this.rawSign = rawSign;
        this.rawEntryType = rawEntryType;
    }

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"transactionName\":\"").append(transactionName).append("\",");
        json.append("\"sign\":\"").append(sign).append("\",");
        json.append("\"entryType\":\"").append(entryType).append("\",");
        json.append("\"accountSubType\":\"").append(accountSubType).append("\",");
        json.append("}");
        return json.toString();
    }

    @Override
    public SubledgerMapping clone() {
        try {
            return (SubledgerMapping) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(); // Should never happen since we are Cloneable
        }
    }
}