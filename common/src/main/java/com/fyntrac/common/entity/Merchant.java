package com.fyntrac.common.entity;

import com.fyntrac.common.enums.MerchantStatus;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "merchants")
public class Merchant {

    @Id
    private String id;

    private String merchantCode;
    private String name;
    private String description;
    private String industryType;

    private String contactEmail;
    private String contactPhone;

    private Address address;
    private List<String> tenantIds;

    private MerchantStatus status;

    private Instant createdAt;
    private Instant updatedAt;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");

        appendJsonField(sb, "id", id, true);
        appendJsonField(sb, "merchantCode", merchantCode, false);
        appendJsonField(sb, "name", name, false);
        appendJsonField(sb, "description", description, false);
        appendJsonField(sb, "industryType", industryType, false);
        appendJsonField(sb, "contactEmail", contactEmail, false);
        appendJsonField(sb, "contactPhone", contactPhone, false);

        // Handle Address object
        sb.append("\"address\":");
        if (address != null) {
            sb.append(address.toString());
        } else {
            sb.append("null");
        }
        sb.append(",");

        // Handle tenantIds list
        sb.append("\"tenantIds\":");
        if (tenantIds != null) {
            sb.append("[");
            for (int i = 0; i < tenantIds.size(); i++) {
                sb.append("\"").append(escapeJson(tenantIds.get(i))).append("\"");
                if (i < tenantIds.size() - 1) sb.append(",");
            }
            sb.append("]");
        } else {
            sb.append("null");
        }
        sb.append(",");

        // Handle Enum field
        sb.append("\"status\":");
        if (status != null) {
            sb.append("\"").append(status.name()).append("\"");
        } else {
            sb.append("null");
        }
        sb.append(",");

        // Handle Instant fields
        appendJsonField(sb, "createdAt", createdAt != null ? createdAt.toString() : null, false);
        appendJsonField(sb, "updatedAt", updatedAt != null ? updatedAt.toString() : null, false);

        // Remove trailing comma
        if (sb.charAt(sb.length() - 1) == ',') {
            sb.setLength(sb.length() - 1);
        }

        sb.append("}");
        return sb.toString();
    }

    private void appendJsonField(StringBuilder sb, String fieldName, String value, boolean isFirst) {
        if (!isFirst) sb.append(",");
        sb.append("\"").append(fieldName).append("\":");
        if (value != null) {
            sb.append("\"").append(escapeJson(value)).append("\"");
        } else {
            sb.append("null");
        }
    }

    private String escapeJson(String value) {
        if (value == null) return null;
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}