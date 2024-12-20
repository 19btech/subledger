package com.fyntrac.common.entity;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@SuperBuilder
@Document(collection = "GeneralLedgerAccountBalance")
public class GeneralLedgerAccountBalance extends AccountBalance implements Serializable {
    @Serial
    private static final long serialVersionUID = -696117065416878924L;
    @Override
    public boolean equals(Object o) {
        if (this == o) return true; // Check for reference equality
        if (!(o instanceof GeneralLedgerAccountBalance)) return false; // Check for the correct type
        GeneralLedgerAccountBalance that = (GeneralLedgerAccountBalance) o; // Cast to the correct type
        return instrumentId == that.instrumentId && // Check field equality
                Objects.equals(code, that.code) &&
                Objects.equals(accountType, that.accountType) &&
                Objects.equals(attributeId, that.attributeId);
    }
    @Override
    public int hashCode() {
        Set<String> hashcode = new HashSet<String>(0);
        hashcode.add(accountType.name());
        hashcode.add(instrumentId);
        hashcode.add(attributeId);
        // hashcode.add(String.valueOf(periodId));
        return hashcode.hashCode();
    }

    public int subCode() {
        Set<String> hashcode = new HashSet<String>(0);
        hashcode.add(accountType.name());
        hashcode.add(instrumentId);
        hashcode.add(attributeId);
        return hashcode.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{")
                .append("\"code\": \"").append(code).append("\", ")
                .append("\"accountType\": ").append(accountType).append(", ")
                .append("\"instrumentId\": ").append(instrumentId).append(", ")
                .append("\"attributeId\": ").append(attributeId)
                .append("\"periodId\": ").append(periodId)
                .append("\"amount\": ").append(amount)
                .append("}");
        return sb.toString();
    }

}

