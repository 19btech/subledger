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
import java.math.RoundingMode;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.format.annotation.NumberFormat;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Digits;
import javax.validation.constraints.NotNull;

@Data
@Builder(builderClassName = "TransactionActivityBuilder")
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "TransactionActivity")
@CompoundIndex(def = "{'instrumentId': 1, 'attributeId': 1}", name = "transaction_activity_instrument_attribute_index")
public class TransactionActivity implements Serializable {
    @Serial
    private static final long serialVersionUID = 8444760102552307163L;

    @Id
    private String id;
    @NotNull
    @Indexed
    private String instrumentId;
    @NotNull
    @Indexed
    private String transactionName;
    @NumberFormat(pattern = "#.####")
    @Digits(integer = 20, fraction = 4)
    @DecimalMax(value = "99999999999999999999.9999")
    @DecimalMin(value = "0.0000")
    private BigDecimal amount;
    @NotNull
    @Indexed
    private String attributeId;
    private int originalPeriodId;
    private long instrumentAttributeVersionId;
    private AccountingPeriod accountingPeriod;
    private int periodId;
    private long batchId;
    private Source source;
    private String sourceId;
    @NotNull
    @Indexed
    private Integer postingDate;
    @NotNull
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

    public void setAmount(BigDecimal amount) {
        this.amount = amount != null ? amount.setScale(4, RoundingMode.HALF_UP) : null;
    }

    public static class TransactionActivityBuilder {
        public TransactionActivityBuilder amount(BigDecimal amount) {
            this.amount = amount != null ? amount.setScale(4, RoundingMode.HALF_UP) : null;
            return this;
        }
    }

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder("{");
        json.append("\"id\":\"").append(id).append("\",");
        try {
            json.append("\"transactionDate\":\"").append(getTransactionDate()).append("\",");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        json.append("\"instrumentId\":\"").append(instrumentId).append("\",");
        json.append("\"transactionName\":\"").append(transactionName).append("\",");
        json.append("\"amount\":").append(amount).append(",");
        json.append("\"attributeId\":\"").append(attributeId).append("\",");
        json.append("\"periodId\":").append(accountingPeriod != null ? accountingPeriod.getPeriodId() : null).append(",");
        json.append("\"originalPeriodId\":").append(originalPeriodId).append(",");
        json.append("\"batchId\":").append(batchId).append(",");
        json.append("\"postingDate\":").append(postingDate).append(",");
        json.append("\"effectiveDate\":").append(effectiveDate).append(",");
        json.append("\"sourceId\":\"").append(sourceId).append("\",");
        json.append("\"instrumentAttributeVersionId\":").append(instrumentAttributeVersionId).append(",");

        json.append("\"attributes\":{");
        if (attributes != null && !attributes.isEmpty()) {
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                json.append("\"").append(entry.getKey()).append("\":\"").append(entry.getValue()).append("\",");
            }
            json.setLength(json.length() - 1); // Remove trailing comma
        }
        json.append("}}");

        return json.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionActivity that)) return false;
        return originalPeriodId == that.originalPeriodId &&
                instrumentAttributeVersionId == that.instrumentAttributeVersionId &&
                Objects.equals(id, that.id) &&
                Objects.equals(postingDate, that.postingDate) &&
                Objects.equals(effectiveDate, that.effectiveDate) &&
                Objects.equals(instrumentId, that.instrumentId) &&
                Objects.equals(transactionName, that.transactionName) &&
                compareBigDecimals(amount, that.amount) &&
                Objects.equals(attributeId, that.attributeId) &&
                Objects.equals(accountingPeriod != null ? accountingPeriod.getPeriodId() : null,
                        that.accountingPeriod != null ? that.accountingPeriod.getPeriodId() : null) &&
                Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, postingDate, effectiveDate, instrumentId, transactionName, scaleSafe(amount),
                attributeId, accountingPeriod != null ? accountingPeriod.getPeriodId() : null,
                originalPeriodId, instrumentAttributeVersionId, attributes);
    }

    private boolean compareBigDecimals(BigDecimal a, BigDecimal b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.setScale(4, RoundingMode.HALF_UP).compareTo(b.setScale(4, RoundingMode.HALF_UP)) == 0;
    }

    private BigDecimal scaleSafe(BigDecimal value) {
        return value != null ? value.setScale(4, RoundingMode.HALF_UP) : null;
    }
}