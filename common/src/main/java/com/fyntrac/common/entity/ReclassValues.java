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
import java.util.Date;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "ReclassValues")
public class ReclassValues implements Serializable {
    @Serial
    private static final long serialVersionUID = -5854328522644513217L;
    @Id
    private String id;
    private String instrumentId;
    private String attributeId;
    private long batchId;
    private long previousVersionId;
    private long currentVersionId;
    private String attributeName;
    private Object oldValue;
    private Object newValue;
    private int previousPeriodId;
    private int currentPeriodId;
    private Date effectiveDate;

    @Field("attributes")
    private Map<String, Object> attributes;

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("ReclassValues{");
        json.append("id='" + id + '\'');
        json.append(", instrumentId='" + instrumentId + '\'');
        json.append(", attributeId='" + attributeId + '\'' );
        json.append(", batchId='" + batchId + '\'' );
        json.append(", previousVersionId=" + previousVersionId);
        json.append(", currentVersionId=" + currentVersionId);
        json.append(", attributeName='" + attributeName + '\'');
        json.append(", oldValue=" + oldValue);
        json.append(", newValue=" + newValue);
        json.append(", previousPeriodId=" + previousPeriodId);
        json.append(", currentPeriodId=" + currentPeriodId);
        json.append(", effectiveDate=" + effectiveDate);
        json.append('}');

        // Add attributes
        json.append("\"attributes\":{");
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            String attributeName = entry.getKey();
            Object attributeValue = entry.getValue();
            json.append("\"").append(attributeName).append("\":\"").append(attributeValue).append("\",");
        }
        // Remove the last comma if attributes are present
        if (!attributes.isEmpty()) {
            json.setLength(json.length() - 1); // Remove last comma
        }
        json.append("}");
        json.append("}");
        return json.toString();
    }
}
