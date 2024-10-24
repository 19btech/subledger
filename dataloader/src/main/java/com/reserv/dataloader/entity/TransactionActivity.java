package com.reserv.dataloader.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "TransactionActivity")
public class TransactionActivity  implements Serializable {
    @Serial
    private static final long serialVersionUID = 8444760102552307163L;

    @MongoId
    String id;
    Date transactionDate;
    String instrumentId;
    String transactionName;
    double value;
    String attributeId;
    int periodId;
    int originalPeriodId;

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"transactionDate\":\"").append(transactionDate).append("\",");
        json.append("\"instrumentId\":\"").append(instrumentId).append("\",");
        json.append("\"transactionName\":\"").append(transactionName).append("\",");
        json.append("\"value\":\"").append(value).append("\",");
        json.append("\"attributeId\":\"").append(attributeId).append("\",");
        json.append("\"originalPeriodId\":\"").append(originalPeriodId).append("\",");
        json.append("}");
        return json.toString();
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + (id == null? 0 : id.hashCode());
        result = 31 * result + (transactionDate == null? 0 : transactionDate.hashCode());
        result = 31 * result + (instrumentId == null? 0 : instrumentId.hashCode());
        result = 31 * result + (transactionName == null? 0 : transactionName.hashCode());
        result = 31 * result + Double.hashCode(value);
        result = 31 * result + (attributeId == null? 0 : attributeId.hashCode());
        result = 31 * result + periodId;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionActivity)) return false;
        TransactionActivity that = (TransactionActivity) o;
        return periodId == that.periodId &&
                Double.compare(that.value, value) == 0 &&
                Objects.equals(id, that.id) &&
                Objects.equals(transactionDate, that.transactionDate) &&
                Objects.equals(instrumentId, that.instrumentId) &&
                Objects.equals(transactionName, that.transactionName) &&
                Objects.equals(attributeId, that.attributeId) &&
                Objects.equals(originalPeriodId,that.originalPeriodId);
    }
}
