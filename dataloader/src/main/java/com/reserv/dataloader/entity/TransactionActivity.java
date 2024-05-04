package com.reserv.dataloader.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;

import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "TransactionActivity")
public class TransactionActivity {
    @MongoId
    String id;
    Date transactionDate;
    String instrumentId;
    String transactionName;
    double amount;
    String attributeId;

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"transactionDate\":\"").append(transactionDate).append("\",");
        json.append("\"instrumentId\":\"").append(instrumentId).append("\",");
        json.append("\"transactionName\":\"").append(transactionName).append("\",");
        json.append("\"amount\":\"").append(amount).append("\",");
        json.append("\"attributeId\":\"").append(attributeId).append("\",");
        json.append("}");
        return json.toString();
    }

}
