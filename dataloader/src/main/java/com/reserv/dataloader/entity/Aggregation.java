package com.reserv.dataloader.entity;


import com.reserv.dataloader.datasource.accounting.rule.AggregationLevel;
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
@Document(collection = "Aggregation")
public class Aggregation implements Serializable {
    @Serial
    private static final long serialVersionUID = 8471241075094548866L;
    @Id
    private String id;
    private String transactionName;
    private String metricName;
    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"transactionName\":\"").append(transactionName).append("\",");
        json.append("\"metricName\":\"").append(metricName).append("\",");
        json.append("}");
        return json.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((transactionName == null) ? 0 : transactionName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Aggregation other = (Aggregation) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (transactionName == null) {
            if (other.transactionName != null)
                return false;
        } else if (!transactionName.equals(other.transactionName))
            return false;
        if (metricName == null) {
            if (other.metricName != null)
                return false;
        } else if (!metricName.equals(other.metricName))
            return false;
        return true;
    }
}
