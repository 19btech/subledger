package com.reserv.dataloader.entity;


import com.reserv.dataloader.datasource.accounting.rule.EntryType;
import com.reserv.dataloader.datasource.accounting.rule.Sign;
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
@Document(collection = "SubledgerMapping")
public class SubledgerMapping {

    @Id
    private String id;
    private String transactionName;
    private Sign sign;
    private EntryType entryType;
    private String accountSubType;

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
}