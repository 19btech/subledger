package com.fyntrac.common.entity;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "users")
public class User {

    @Id
    private String id;

    private String username;
    private String email;
    private String passwordHash;

    private String firstName;
    private String lastName;
    private String phoneNumber;

    private List<String> tenantIds;
    private String merchantId;

    private List<UserRole> roles;
    private boolean verified;
    private boolean active;

    private Instant lastLoginAt;
    private Instant createdAt;
    private Instant updatedAt;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");

        appendJsonField(sb, "id", id, true);
        appendJsonField(sb, "username", username, false);
        appendJsonField(sb, "email", email, false);

        // Exclude passwordHash from toString for security
        sb.append("\"passwordHash\":\"[PROTECTED]\",");

        appendJsonField(sb, "firstName", firstName, false);
        appendJsonField(sb, "lastName", lastName, false);
        appendJsonField(sb, "phoneNumber", phoneNumber, false);

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

        appendJsonField(sb, "merchantId", merchantId, false);

        // Handle roles list
        sb.append("\"roles\":");
        if (roles != null) {
            sb.append("[");
            for (int i = 0; i < roles.size(); i++) {
                UserRole role = roles.get(i);
                sb.append("{\"name\":\"").append(escapeJson(role.getName())).append("\"");
                sb.append(",\"description\":\"").append(escapeJson(role.getDescription())).append("\"");
                sb.append("}");
                if (i < roles.size() - 1) sb.append(",");
            }
            sb.append("]");
        } else {
            sb.append("null");
        }
        sb.append(",");

        // Handle boolean fields
        sb.append("\"verified\":").append(verified).append(",");
        sb.append("\"active\":").append(active).append(",");

        // Handle Instant fields
        appendJsonField(sb, "lastLoginAt", lastLoginAt != null ? lastLoginAt.toString() : null, false);
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

    private String listToJson(List<String> list) {
        if (list == null) return "null";
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            sb.append("\"").append(escapeJson(list.get(i))).append("\"");
            if (i < list.size() - 1) sb.append(",");
        }
        sb.append("]");
        return sb.toString();
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