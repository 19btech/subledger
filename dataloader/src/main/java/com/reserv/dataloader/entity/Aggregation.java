package com.reserv.dataloader.entity;


import com.reserv.dataloader.datasource.accounting.rule.AggregationLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "Aggregation")
public class Aggregation {

    @Id
    private String id;
    private String transactionId;
    private String transactionName;
    private String metricName;
    private AggregationLevel level;

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"transactionId\":\"").append(transactionId).append("\",");
        json.append("\"transactionName\":\"").append(transactionName).append("\",");
        json.append("\"metricName\":\"").append(metricName).append("\",");
        json.append("\"level\":\"").append(level).append("\"");
        json.append("}");
        return json.toString();
    }
}
