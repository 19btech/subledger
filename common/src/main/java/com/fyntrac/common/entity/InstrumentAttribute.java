package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "InstrumentAttribute")
public class InstrumentAttribute {
    @Id
    private String id;
    private Date effectiveDate;
    private String instrumentId;
    private String attributeId;
    @Field("attributes")
    private Map<String,Object> attributes;

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"effectiveDate\":\"").append(effectiveDate).append("\",");
        json.append("\"instrumentId\":\"").append(instrumentId).append("\",");
        json.append("\"attributeId\":\"").append(attributeId).append("\",");
        json.append("attributes: [");
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            // Print the key and value
            json.append("{");
            json.append("\"").append(key).append("\":\"").append(value).append("\",");
            json.append("}");
        }
        json.append("]");
        json.append("}");
        return json.toString();
    }

}
