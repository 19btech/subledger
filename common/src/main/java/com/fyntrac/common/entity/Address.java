package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Address {
    private String street;
    private String city;
    private String state;
    private String postalCode;
    private String country;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");

        appendJsonField(sb, "street", street, true);
        appendJsonField(sb, "city", city, false);
        appendJsonField(sb, "state", state, false);
        appendJsonField(sb, "postalCode", postalCode, false);
        appendJsonField(sb, "country", country, false);

        // Remove trailing comma if last field was null
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