package com.fyntrac.common.entity;


import com.fyntrac.common.enums.DataType;
import org.springframework.data.annotation.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "Attributes")
public class Attributes {
    @Id
    private String id;
    private String userField;
    private String attributeName;
    private int isReclassable;
    private DataType dataType;
    private int isNullable;

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"userField\":\"").append(userField).append("\",");
        json.append("\"attributeName\":\"").append(attributeName).append("\",");
        json.append("\"isReclassable\":").append(isReclassable).append(",");
        json.append("\"dataType\":\"").append(dataType).append("\",");
        json.append("\"isNullable\":").append(isNullable);
        json.append("}");
        return json.toString();
    }
}
