package com.fyntrac.common.entity;

import com.fyntrac.common.service.SequenceGeneratorService;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "InstrumentAttribute")
@CompoundIndex(def = "{'attributeId': 1, 'instrumentId': 1}", name = "attribute_instrument_index")
public class InstrumentAttribute {
    @Id
    private String id;
    private Date effectiveDate;
    private String instrumentId;
    private String attributeId;
    private Date endDate;
    private Date originationDate;
    private int periodId;
    @Indexed // Separate index on versionId
    private int versionId;
    @Field("attributes")
    private Map<String,Object> attributes;

    @Autowired
    private SequenceGeneratorService sequenceGeneratorService;

    public void generateVersionId() {
        this.versionId = sequenceGeneratorService.generateInstrumentAttributeVersionId();
    }

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"effectiveDate\":\"").append(effectiveDate).append("\",");
        json.append("\"instrumentId\":\"").append(instrumentId).append("\",");
        json.append("\"attributeId\":\"").append(attributeId).append("\",");
        json.append("\"endDate\":\"").append(endDate).append("\",");
        json.append("\"originationDate\":\"").append(originationDate).append("\",");
        json.append("\"periodId\":").append(periodId).append(",");
        json.append("\"versionId\":").append(versionId).append(",");
        json.append("\"attributes\":{");

        if (attributes != null && !attributes.isEmpty()) {
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // Print the key and value
                json.append("\"").append(key).append("\":\"").append(value).append("\",");
            }
            // Remove the last comma
            json.setLength(json.length() - 1);
        }

        json.append("}");
        json.append("}");
        return json.toString();
    }

}
