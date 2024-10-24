package com.fyntrac.common.entity;

import com.fyntrac.common.enums.AccountType;
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
@Document(collection = "AccountTypes")
public class AccountTypes {
    @Id
    private String id;
    private String accountSubType;
    private AccountType accountType;

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"accountSubType\":\"").append(accountSubType).append("\",");
        json.append("\"accountType\":\"").append(accountType).append("\",");
        json.append("}");
        return json.toString();
    }
}