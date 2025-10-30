package com.fyntrac.common.service;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.EventConfiguration;
import com.fyntrac.common.mapper.EventConfigurationMapper;
import com.fyntrac.common.repository.EventConfigurationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
@Transactional
public class EventConfigurationService {

    private static final Logger logger = LoggerFactory.getLogger(EventConfigurationService.class);

    private final EventConfigurationRepository eventConfigurationRepository;
    private final EventConfigurationMapper eventConfigurationMapper;

    public EventConfigurationService(EventConfigurationRepository eventConfigurationRepository,
                                     EventConfigurationMapper eventConfigurationMapper) {
        this.eventConfigurationRepository = eventConfigurationRepository;
        this.eventConfigurationMapper = eventConfigurationMapper;
    }

    public Records.EventConfigurationResponse createEventConfiguration(Records.EventConfigurationRequest request, String userId) {
        logger.info("Creating/Updating event configuration for event ID: {}", request.eventId());

        // Check if event configuration already exists with this eventId
        Optional<EventConfiguration> existingConfig = this.eventConfigurationRepository.findByEventId(
                request.eventId());

        if (existingConfig.isPresent()) {
            // Update existing configuration
            logger.info("Event ID already exists: {}. Updating existing configuration.", request.eventId());
            EventConfiguration existingEntity = existingConfig.get();

            // Update the existing entity with new data
            EventConfiguration updatedEntity = this.eventConfigurationMapper.updateEntityFromRequest(
                    existingEntity, request, userId);

            EventConfiguration saved = (EventConfiguration) this.eventConfigurationRepository.save(updatedEntity);
            logger.info("Event configuration updated successfully with ID: {}", saved.getId());
            return this.eventConfigurationMapper.toResponse(saved);
        } else {
            // Create new configuration
            EventConfiguration entity = this.eventConfigurationMapper.toEntity(request, this.getCurrentTenantId(), userId);
            EventConfiguration saved = (EventConfiguration) this.eventConfigurationRepository.save(entity);
            logger.info("Event configuration created successfully with ID: {}", saved.getId());
            return this.eventConfigurationMapper.toResponse(saved);
        }
    }

    @Transactional(readOnly = true)
    public Records.EventConfigurationResponse getEventConfigurationById(String id) {
        logger.info("Fetching event configuration by ID: {}", id);

        EventConfiguration entity = eventConfigurationRepository.findById(id)
                .orElseThrow(() -> {
                    logger.warn("Event configuration not found with id: {}", id);
                    return new RuntimeException("Event configuration not found with id: " + id);
                });

        validateTenantAccess(entity);
        return eventConfigurationMapper.toResponse(entity);
    }

    @Transactional(readOnly = true)
    public Records.EventConfigurationResponse getEventConfigurationByEventId(String eventId) {
        logger.info("Fetching event configuration by event ID: {}", eventId);

        EventConfiguration entity = eventConfigurationRepository.findByEventId(eventId)
                .orElseThrow(() -> {
                    logger.warn("Event configuration not found with event ID: {}", eventId);
                    return new RuntimeException("Event configuration not found with event ID: " + eventId);
                });
        return eventConfigurationMapper.toResponse(entity);
    }

    @Transactional(readOnly = true)
    public List<Records.EventConfigurationResponse> getAllEventConfigurations() {
        logger.info("Fetching all event configurations for tenant");

        List<EventConfiguration> entities = eventConfigurationRepository.findAll();
        logger.info("Found {} event configurations", entities.size());

        return entities.stream()
                .map(eventConfigurationMapper::toResponse)
                .toList();
    }

    @Transactional(readOnly = true)
    public List<Records.EventConfigurationBasicResponse> getEventConfigurationsBasicInfo() {
        logger.info("Fetching event configurations basic info for tenant");

        List<EventConfiguration> entities = eventConfigurationRepository.findByIsActive(Boolean.TRUE);
        return entities.stream()
                .map(this::toBasicResponse)
                .toList();
    }

    public Records.EventConfigurationResponse updateEventConfigurationStatus(String eventId,
                                                                        boolean isActive, String userId) {
        logger.info("Updating event configuration with ID: {}", eventId);

        EventConfiguration entity = eventConfigurationRepository.findById(eventId)
                .orElseThrow(() -> {
                    logger.warn("Event configuration not found with id: {}", eventId);
                    return new RuntimeException("Event configuration not found with id: " + eventId);
                });

        validateTenantAccess(entity);

        // Check if event ID is being changed and if new one already exists
        if (!entity.getEventId().equals(eventId) &&
                eventConfigurationRepository.existsByEventId(eventId)) {
            logger.warn("Event ID already exists: {}", eventId);
            throw new IllegalArgumentException("Event ID already exists: " + eventId);
        }

        entity.setIsActive(isActive);
        EventConfiguration updated = eventConfigurationRepository.save(entity);
        logger.info("Event configuration updated successfully with ID: {}", updated.getId());

        return eventConfigurationMapper.toResponse(updated);
    }

    public Records.EventConfigurationResponse updateEventConfiguration(String id, Records.EventConfigurationRequest request, String userId) {
        logger.info("Updating event configuration with ID: {}", id);

        EventConfiguration entity = eventConfigurationRepository.findById(id)
                .orElseThrow(() -> {
                    logger.warn("Event configuration not found with id: {}", id);
                    return new RuntimeException("Event configuration not found with id: " + id);
                });

        validateTenantAccess(entity);

        // Check if event ID is being changed and if new one already exists
        if (!entity.getEventId().equals(request.eventId()) &&
                eventConfigurationRepository.existsByEventId(request.eventId())) {
            logger.warn("Event ID already exists: {}", request.eventId());
            throw new IllegalArgumentException("Event ID already exists: " + request.eventId());
        }

        eventConfigurationMapper.updateEntity(entity, request, userId);
        EventConfiguration updated = eventConfigurationRepository.save(entity);
        logger.info("Event configuration updated successfully with ID: {}", updated.getId());

        return eventConfigurationMapper.toResponse(updated);
    }

    public void deleteEventConfiguration(String id, String userId) {
        logger.info("Soft deleting event configuration with ID: {}", id);

        EventConfiguration entity = eventConfigurationRepository.findById(id)
                .orElseThrow(() -> {
                    logger.warn("Event configuration not found with id: {}", id);
                    return new RuntimeException("Event configuration not found with id: " + id);
                });

        validateTenantAccess(entity);

        entity.setIsActive(false);
        entity.setUpdatedBy(userId);
        entity.setUpdatedAt(LocalDateTime.now());
        eventConfigurationRepository.save(entity);
        logger.info("Event configuration soft deleted successfully with ID: {}", id);
    }

    @Transactional(readOnly = true)
    public boolean existsByEventId(String eventId) {
        return eventConfigurationRepository.existsByEventId(eventId);
    }

    private Records.EventConfigurationBasicResponse toBasicResponse(EventConfiguration entity) {
        return RecordFactory.createEventConfigurationBasicResponseRecord(
                entity.getId(),
                entity.getEventId(),
                entity.getEventName(),
                entity.getPriority(),
                entity.getDescription(),
                entity.getCreatedAt()
        );
    }

    private void validateTenantAccess(EventConfiguration entity) {
        if (equals(getCurrentTenantId() == null || getCurrentTenantId().isEmpty())) {
            logger.warn("Access denied to event configuration. Tenant not found: {}",
                    getCurrentTenantId());
            throw new RuntimeException("Access denied to event configuration");
        }
    }

    private String getCurrentTenantId() {
        // Get tenant from context
        String tenantId = com.fyntrac.common.config.TenantContextHolder.getTenant();
        if (tenantId == null) {
            logger.warn("No tenant ID found in context");
            throw new IllegalStateException("No tenant set in context");
        }
        return tenantId;
    }
}