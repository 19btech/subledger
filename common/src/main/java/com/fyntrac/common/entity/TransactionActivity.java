package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

import org.springframework.data.annotation.Id;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "TransactionActivity")
public class TransactionActivity implements Serializable {
    @Serial
    private static final long serialVersionUID = 8444760102552307163L;

    @Id
    private String id;
    private Date transactionDate;
    private String instrumentId;
    private String transactionName;
    private double amount;
    private String attributeId;
    private int periodId;
    private int originalPeriodId;
    private long instrumentAttributeVersionId;

    @Field("attributes")
    private Map<String, Object> attributes;

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"transactionDate\":\"").append(transactionDate).append("\",");
        json.append("\"instrumentId\":\"").append(instrumentId).append("\",");
        json.append("\"transactionName\":\"").append(transactionName).append("\",");
        json.append("\"amount\":").append(amount).append(","); // Corrected to "amount"
        json.append("\"attributeId\":\"").append(attributeId).append("\",");
        json.append("\"periodId\":").append(periodId).append(",");
        json.append("\"originalPeriodId\":").append(originalPeriodId).append(",");
        json.append("\"instrumentAttributeVersionId\":").append(instrumentAttributeVersionId).append(",");

        // Add attributes
        json.append("\"attributes\":{");
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            String attributeName = entry.getKey();
            Object attributeValue = entry.getValue();
            json.append("\"").append(attributeName).append("\":\"").append(attributeValue).append("\",");
        }
        // Remove the last comma if attributes are present
        if (!attributes.isEmpty()) {
            json.setLength(json.length() - 1); // Remove last comma
        }
        json.append("}");
        json.append("}");
        return json.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, transactionDate, instrumentId, transactionName, amount, attributeId, periodId, originalPeriodId, instrumentAttributeVersionId, attributes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionActivity)) return false;
        TransactionActivity that = (TransactionActivity) o;
        return periodId == that.periodId &&
                Double.compare(that.amount, amount) == 0 &&
                originalPeriodId == that.originalPeriodId &&
                instrumentAttributeVersionId == that.instrumentAttributeVersionId &&
                Objects.equals(id, that.id) &&
                Objects.equals(transactionDate, that.transactionDate) &&
                Objects.equals(instrumentId, that.instrumentId) &&
                Objects.equals(transactionName, that.transactionName) &&
                Objects.equals(attributeId, that.attributeId) &&
                Objects.equals(attributes, that.attributes);
    }
}