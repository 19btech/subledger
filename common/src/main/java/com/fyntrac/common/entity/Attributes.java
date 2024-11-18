package com.fyntrac.common.entity;


import com.fyntrac.common.enums.DataType;
import org.springframework.data.annotation.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "Attributes")
public class Attributes implements Serializable {
    @Serial
    private static final long serialVersionUID = 44158520015183786L;
    @Id
    private String id;
    private String userField;
    private String attributeName;
    private int isReclassable;
    private DataType dataType;
    private int isNullable;
    private long sequenceId;

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"userField\":\"").append(userField).append("\",");
        json.append("\"attributeName\":\"").append(attributeName).append("\",");
        json.append("\"isReclassable\":").append(isReclassable).append(",");
        json.append("\"dataType\":\"").append(dataType).append("\",");
        json.append("\"isNullable\":").append(isNullable).append(",");
        json.append("\"sequence\":").append(sequenceId);
        json.append("}");
        return json.toString();
    }
}
