package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "ChartOfAccount")
public class ChartOfAccount implements Serializable {
    @Serial
    private static final long serialVersionUID= 2663037811309659882L;

    @Id
    private String id;
    private String accountNumber;
    private String accountName;
    private String accountSubtype;

    @Field("attributes")
    private Map<String,Object> attributes;

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("id:'").append(id).append("',");
        json.append("accountNumber:'").append(accountNumber).append("',");
        json.append("accountName:'").append(accountName).append("',");
        json.append("accountSubtype:'").append(accountSubtype).append("',");
        for(Map.Entry<String, Object> entry : attributes.entrySet()) {
            String attributeName = entry.getKey();
            Object attibuteValue = entry.getValue();
            json.append(attributeName).append(":'").append(attibuteValue).append("',");
        }
        json.append("}");
        return json.toString();
    }

}
