package com.fyntrac.common.mapper;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.EventConfiguration;
import com.fyntrac.common.entity.Option;
import com.fyntrac.common.entity.SourceMapping;
import com.fyntrac.common.entity.TriggerSetup;
import com.fyntrac.common.enums.FieldType;
import com.fyntrac.common.enums.SourceTable;
import com.fyntrac.common.enums.TriggerType;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.util.List;

@Component
public class EventConfigurationMapper {

    public EventConfiguration toEntity(Records.EventConfigurationRequest request, String tenantId, String createdBy) {
        EventConfiguration entity = new EventConfiguration();
        entity.setEventId(request.eventId());
        entity.setEventName(request.eventName());
        entity.setPriority(request.priority());
        entity.setDescription(request.description());
        entity.setTriggerSetup(toTriggerSetup(request.triggerSetup()));
        entity.setSourceMappings(toSourceMappings(request.sourceMappings()));
        entity.setCreatedBy(createdBy);
        entity.setUpdatedBy(createdBy);
        return entity;
    }

    public EventConfiguration updateEntityFromRequest(EventConfiguration existingEntity,
                                                      Records.EventConfigurationRequest request,
                                                      String userId) {
        // Update all fields from the request
        existingEntity.setEventName(request.eventName());
        existingEntity.setPriority(request.priority());
        existingEntity.setDescription(request.description());
        existingEntity.setTriggerSetup(toTriggerSetup(request.triggerSetup()));
        existingEntity.setSourceMappings(toSourceMappings(request.sourceMappings()));
        existingEntity.setUpdatedBy(userId);
        existingEntity.setUpdatedAt(LocalDateTime.now());

        return existingEntity;
    }

    public void updateEntity(EventConfiguration entity, Records.EventConfigurationRequest request, String updatedBy) {
        entity.setEventId(request.eventId());
        entity.setEventName(request.eventName());
        entity.setPriority(request.priority());
        entity.setDescription(request.description());
        entity.setTriggerSetup(toTriggerSetup(request.triggerSetup()));
        entity.setSourceMappings(toSourceMappings(request.sourceMappings()));
        entity.setUpdatedBy(updatedBy);
        entity.setUpdatedAt(java.time.LocalDateTime.now());
    }


    private TriggerSetup toTriggerSetup(Records.TriggerSetupRequest request) {
        // Convert string trigger type to enum
        TriggerType triggerType;
        try {
            triggerType = TriggerType.valueOf(request.triggerType());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid trigger type: " + request.triggerType());
        }

        return new TriggerSetup(
                triggerType,
                request.triggerCondition(),
                toOptions(request.triggerSource())
        );
    }

    private List<SourceMapping> toSourceMappings(List<Records.SourceMappingRequest> requests) {
        return requests.stream()
                .map(this::toSourceMapping)
                .toList();
    }

    private SourceMapping toSourceMapping(Records.SourceMappingRequest request) {
        // Convert string source table to enum
        String sourceTable;
        FieldType fieldType;
        try {
            sourceTable = request.sourceTable();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid source table: " + request.sourceTable());
        }

        try {
            if(request.fieldType() != null && !(request.fieldType().isEmpty())) {
                fieldType = FieldType.valueOf(request.fieldType().toUpperCase());
            }else{
                fieldType = FieldType.NONE;
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid filed type: " + request.fieldType());
        }

        return new SourceMapping(
                sourceTable,
                toOptions(request.sourceColumns()),
                toOptions(request.versionType()),
                toOptions(request.dataMapping()),
                fieldType
        );
    }

    private List<Option> toOptions(List<Records.OptionRequest> requests) {
        if (requests == null) {
            return List.of();
        }
        return requests.stream()
                .map(req -> new Option(req.label(), req.value()))
                .toList();
    }

    public Records.EventConfigurationResponse toResponse(EventConfiguration entity) {
        return RecordFactory.createEventConfigurationResponseRecord(
                entity.getId(),
                entity.getEventId(),
                entity.getEventName(),
                entity.getPriority(),
                entity.getDescription(),
                toTriggerSetupResponse(entity.getTriggerSetup()),
                toSourceMappingResponses(entity.getSourceMappings()),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                entity.getCreatedBy(),
                entity.getUpdatedBy(),
                entity.getIsActive(),
                entity.getIsDeleted()
        );
    }

    private Records.TriggerSetupResponse toTriggerSetupResponse(TriggerSetup triggerSetup) {
        return RecordFactory.createTriggerSetupResponseRecord(
                triggerSetup.getTriggerType().name(),
                triggerSetup.getTriggerCondition(),
                toOptionResponses(triggerSetup.getTriggerSource())
        );
    }

    private List<Records.SourceMappingResponse> toSourceMappingResponses(List<SourceMapping> sourceMappings) {
        return sourceMappings.stream()
                .map(this::toSourceMappingResponse)
                .toList();
    }

    private Records.SourceMappingResponse toSourceMappingResponse(SourceMapping sourceMapping) {
        return RecordFactory.createSourceMappingResponseRecord(
                sourceMapping.getSourceTable(),
                toOptionResponses(sourceMapping.getSourceColumns()),
                toOptionResponses(sourceMapping.getVersionType()),
                toOptionResponses(sourceMapping.getDataMapping()),
                sourceMapping.getFieldType().getDisplayName()
        );
    }

    private List<Records.OptionResponse> toOptionResponses(List<Option> options) {
        if (options == null) {
            return List.of();
        }
        return options.stream()
                .map(opt -> RecordFactory.createOptionResponseRecord(opt.getLabel(), opt.getValue()))
                .toList();
    }
}