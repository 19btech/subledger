package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;
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
    private long previousVersionId;
    private long currentVersionId;
    private String attributeName;
    private Object oldValue;
    private Object newValue;
    private int previousPeriodId;
    private int currentPeriodId;

    @Override
    public String toString() {
        return "ReclassValues{" +
                "id='" + id + '\'' +
                ", instrumentId='" + instrumentId + '\'' +
                ", attributeId='" + attributeId + '\'' +
                ", previousVersionId=" + previousVersionId +
                ", currentVersionId=" + currentVersionId +
                ", attributeName='" + attributeName + '\'' +
                ", oldValue=" + oldValue +
                ", newValue=" + newValue +
                ", previousPeriodId=" + previousPeriodId +
                ", currentPeriodId=" + currentPeriodId +
                '}';
    }
}
