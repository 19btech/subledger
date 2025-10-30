package com.fyntrac.common.entity;

import com.fyntrac.common.utils.StringUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor  // Generates no-args constructor
@AllArgsConstructor // Generates all-args constructor for @Builder
@Document(collection = "EventConfigurations")
public class EventConfiguration {

    @Id
    private String id;

    @Field("eventId")
    private String eventId;

    @Field("eventName")
    private String eventName;

    @Field("eventPriority")
    private Integer priority;

    @Field("description")
    private String description;

    @Field("triggerSetup")
    private TriggerSetup triggerSetup;

    @Field("sourceMappings")
    private List<SourceMapping> sourceMappings;

    @Field("createdAt")
    private LocalDateTime createdAt;

    @Field("updatedAt")
    private LocalDateTime updatedAt;

    @Field("createdBy")
    private String createdBy;

    @Field("updatedBy")
    private String updatedBy;

    @Field("isActive")
    @Builder.Default
    private Boolean isActive = Boolean.TRUE;

    // Custom constructors (optional, but ensure they don't conflict)
    public EventConfiguration(String eventId, String eventName, Integer priority, String description,
                              TriggerSetup triggerSetup, List<SourceMapping> sourceMappings) {
        this();  // Calls no-args constructor to set defaults
        this.eventId = eventId;
        this.eventName = eventName;
        this.priority = priority;
        this.description = description;
        this.triggerSetup = triggerSetup;
        this.sourceMappings = sourceMappings;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    // JSON-compatible toString method (manual implementation without Jackson)
    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        // Add fields
        StringUtil.addField(json, "id", id);
        StringUtil.addField(json, "eventId", eventId);
        StringUtil.addField(json, "eventName", eventName);
        StringUtil.addField(json, "priority", priority);
        StringUtil.addField(json, "description", description);
        StringUtil.addField(json, "triggerSetup", triggerSetup != null ? triggerSetup.toString() : null);
        StringUtil.addListField(json, "sourceMappings", sourceMappings);
        StringUtil.addField(json, "createdAt", createdAt != null ? createdAt.toString() : null);
        StringUtil.addField(json, "updatedAt", updatedAt != null ? updatedAt.toString() : null);
        StringUtil.addField(json, "createdBy", createdBy);
        StringUtil.addField(json, "updatedBy", updatedBy);
        StringUtil.addField(json, "isActive", isActive, true);  // Last field, no comma
        json.append("}");
        return json.toString();
    }
}
