package com.reserv.dataloader.controller;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.service.EventConfigurationService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

@RestController
@RequestMapping("/api/dataloader/fyntrac/event-configurations")
@CrossOrigin(origins = "*")
public class EventConfigurationController {

    private final EventConfigurationService eventConfigurationService;

    public EventConfigurationController(EventConfigurationService eventConfigurationService) {
        this.eventConfigurationService = eventConfigurationService;
    }

    @PostMapping("/create")
    public ResponseEntity<Records.EventConfigurationResponse> createEventConfiguration(
            @Valid @RequestBody Records.EventConfigurationRequest request,
            @RequestHeader("X-User-Id") String userId) {

        Records.EventConfigurationResponse response = eventConfigurationService.createEventConfiguration(request, userId);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/get/{id}")
    public ResponseEntity<Records.EventConfigurationResponse> getEventConfigurationById(@PathVariable String id) {
        Records.EventConfigurationResponse response = eventConfigurationService.getEventConfigurationByEventId(id);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/all")
    public ResponseEntity<List<Records.EventConfigurationResponse>> getAllEventConfigurations() {
        List<Records.EventConfigurationResponse> responses = eventConfigurationService.getAllEventConfigurations();
        return ResponseEntity.ok(responses);
    }

    @GetMapping("/basic-info")
    public ResponseEntity<List<Records.EventConfigurationBasicResponse>> getEventConfigurationsBasicInfo() {
        List<Records.EventConfigurationBasicResponse> responses = eventConfigurationService.getEventConfigurationsBasicInfo();
        return ResponseEntity.ok(responses);
    }

    @PutMapping("/update/{id}")
    public ResponseEntity<Records.EventConfigurationResponse> updateEventConfiguration(
            @PathVariable String id,
            @Valid @RequestBody Records.EventConfigurationRequest request,
            @RequestHeader("X-User-Id") String userId) {

        Records.EventConfigurationResponse response = eventConfigurationService.updateEventConfiguration(id, request, userId);
        return ResponseEntity.ok(response);
    }

    @PutMapping("/update/status/{id}/{isActive}")
    public ResponseEntity<Records.EventConfigurationResponse> updateEventConfigurationStatus(
            @PathVariable String id,
            @PathVariable boolean isActive,
            @RequestHeader("X-User-Id") String userId) {

        Records.EventConfigurationResponse response = eventConfigurationService.updateEventConfigurationStatus(id,
                isActive, userId);
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<Void> deleteEventConfiguration(
            @PathVariable String id,
            @RequestHeader("X-User-Id") String userId) {

        eventConfigurationService.deleteEventConfiguration(id, userId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/event-id/{eventId}")
    public ResponseEntity<Records.EventConfigurationResponse> getEventConfigurationByEventId(@PathVariable String eventId) {
        Records.EventConfigurationResponse response = eventConfigurationService.getEventConfigurationByEventId(eventId);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/exists/{eventId}")
    public ResponseEntity<Boolean> checkEventIdExists(@PathVariable String eventId) {
        boolean exists = eventConfigurationService.existsByEventId(eventId);
        return ResponseEntity.ok(exists);
    }

    @GetMapping("/health")
    public ResponseEntity<String> healthCheck() {
        return ResponseEntity.ok("Event Configuration Service is healthy");
    }
}