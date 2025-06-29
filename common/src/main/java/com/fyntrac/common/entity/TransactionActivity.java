package com.fyntrac.common.entity;

import com.fyntrac.common.enums.Source;
import com.fyntrac.common.utils.DateUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.format.annotation.NumberFormat;

import javax.validation.constraints.NotNull;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "TransactionActivity")
@CompoundIndex(def = "{'instrumentId': 1, 'attributeId': 1}", name = "transaction_activity_instrument_attribute_index")
public class TransactionActivity implements Serializable {
    @Serial
    private static final long serialVersionUID = 8444760102552307163L;

    @Id
    private String id;
    @Indexed
    @Field(targetType = FieldType.DATE_TIME)
    private Date transactionDate;
    private String instrumentId;
    private String transactionName;
    @NumberFormat(pattern = "#.####")
    private BigDecimal amount;
    private String attributeId;
    private int originalPeriodId;
    private long instrumentAttributeVersionId;
    private AccountingPeriod accountingPeriod;
    private long batchId;
    private Source source;
    private String sourceId;
    @NotNull
    @Indexed
    private Integer postingDate;
    @Indexed
    private Integer effectiveDate;
    @Field("attributes")
    private Map<String, Object> attributes;
    int isReplayable;

    /**
     * Get Transaction Date
     * @return UTC Date or null if effectiveDate is null
     */
    public Date getTransactionDate() throws ParseException {
        if (this.effectiveDate == null || this.effectiveDate == 0) {
            return null;
        }
        return DateUtil.convertIntDateToUtc(this.effectiveDate);
    }

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"transactionDate\":\"").append(transactionDate).append("\",");
        json.append("\"instrumentId\":\"").append(instrumentId).append("\",");
        json.append("\"transactionName\":\"").append(transactionName).append("\",");
        json.append("\"amount\":").append(amount).append(","); // Corrected to "amount"
        json.append("\"attributeId\":\"").append(attributeId).append("\",");
        json.append("\"periodId\":").append(accountingPeriod).append(",");
        json.append("\"originalPeriodId\":").append(originalPeriodId).append(",");
        json.append("\"batchId\":").append(batchId).append(",");
        json.append("\"postingDate\":").append(postingDate).append(",");
        json.append("\"effectiveDate\":").append(effectiveDate).append(",");
        json.append("\"source\":").append(sourceId).append(",");
        json.append("\"instrumentAttributeVersionId\":").append(instrumentAttributeVersionId).append(",");

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

    @Override
    public int hashCode() {
        return Objects.hash(id, transactionDate, instrumentId, transactionName, amount, attributeId, this.accountingPeriod.getPeriodId(), originalPeriodId, instrumentAttributeVersionId, attributes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionActivity)) return false;
        TransactionActivity that = (TransactionActivity) o;
        return this.accountingPeriod.getPeriodId() == that.accountingPeriod.getPeriodId() &&
                that.amount.compareTo(amount) == 0 &&
                originalPeriodId == that.originalPeriodId &&
                instrumentAttributeVersionId == that.instrumentAttributeVersionId &&
                Objects.equals(id, that.id) &&
                Objects.equals(transactionDate, that.transactionDate) &&
                Objects.equals(instrumentId, that.instrumentId) &&
                Objects.equals(transactionName, that.transactionName) &&
                Objects.equals(attributeId, that.attributeId) &&
                Objects.equals(attributes, that.attributes);
    }

    public int getPeriodId() {
        return this.accountingPeriod.getPeriodId();
    }
}