package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;
import org.springframework.data.annotation.Id;
import java.util.Objects;

/**
 * Represents a general ledger entry stage in the accounting system.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "GeneralLedgerEntery")
public class GeneralLedgerEntery implements Serializable {
    @Serial
    private static final long serialVersionUID = -5733755571828897821L;

    @Id
    private String id;
    private String attributeId;
    private String instrumentId;
    private String transactionName;
    private Date transactionDate;
    private int periodId;
    private String glAccountNumber;
    private String glAccountName;
    private String glAccountType;
    private String glAccountSubType;
    private BigDecimal debitAmount;
    private BigDecimal creditAmount;
    private int isReclass;

    @Field("attributes")
    private Map<String, Object> attributes;

    /**
     * Returns a string representation of the GeneralLedgerEnteryStage object.
     *
     * @return a JSON-like string representation of the object
     */
    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{")
                .append("\"id\":\"").append(id).append("\",")
                .append("\"attributeId\":\"").append(attributeId).append("\",")
                .append("\"instrumentId\":\"").append(instrumentId).append("\",")
                .append("\"transactionName\":\"").append(transactionName).append("\",")
                .append("\"transactionDate\":\"").append(transactionDate).append("\",")
                .append("\"periodId\":").append(periodId).append(",")
                .append("\"glAccountNumber\":\"").append(glAccountNumber).append("\",")
                .append("\"glAccountName\":\"").append(glAccountName).append("\",")
                .append("\"glAccountType\":\"").append(glAccountType).append("\",")
                .append("\"glAccountSubType\":\"").append(glAccountSubType).append("\",")
                .append("\"debitAmount\":").append(debitAmount).append(",")
                .append("\"creditAmount\":").append(creditAmount).append(",")
                .append("\"isReclass\":").append(isReclass).append(",")
                .append("\"attributes\":").append(attributes)
                .append("}");
        return json.toString();
    }

    /**
     * Computes a hash code for the GeneralLedgerEnteryStage object.
     *
     * @return a hash code value for this object
     */
    @Override
    public int hashCode() {
        return Objects.hash(id, attributeId, instrumentId, transactionName, transactionDate, periodId,
                glAccountNumber, glAccountName, glAccountType, glAccountSubType,
                debitAmount, creditAmount, isReclass, attributes);
    }

    /**
     * Indicates whether some other object is "equal to" this GeneralLedgerEnteryStage.
     *
     * @param o the reference object with which to compare
     * @return true if this object is the same as the obj argument; false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GeneralLedgerEntery)) return false;
        GeneralLedgerEntery that = (GeneralLedgerEntery) o;
        return periodId == that.periodId &&
                that.debitAmount.compareTo(debitAmount) == 0 &&
                that.creditAmount.compareTo(creditAmount) == 0 &&
                isReclass == that.isReclass &&
                Objects.equals(id, that.id) &&
                Objects.equals(attributeId, that.attributeId) &&
                Objects.equals(instrumentId, that.instrumentId) &&
                Objects.equals(transactionName, that.transactionName) &&
                Objects.equals(transactionDate, that.transactionDate) &&
                Objects.equals(glAccountNumber, that.glAccountNumber) &&
                Objects.equals(glAccountName, that.glAccountName) &&
                Objects.equals(glAccountType, that.glAccountType) &&
                Objects.equals(glAccountSubType, that.glAccountSubType) &&
                Objects.equals(attributes, that.attributes);
    }
}
