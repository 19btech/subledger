package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.entity.Option;
import com.fyntrac.common.service.AttributeService;
import com.fyntrac.common.service.DataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

@RestController
@RequestMapping("/api/dataloader/attribute")
@Slf4j
public class AttributeController {

    private final DataService dataService;
    private final AttributeService attributeService;
    private final com.reserv.dataloader.validation.AttributesValidator validator;

    @Autowired
    public AttributeController(
            DataService dataService, 
            AttributeService attributeService,
            com.reserv.dataloader.validation.AttributesValidator validator) {
        this.dataService = dataService;
        this.attributeService = attributeService;
        this.validator = validator;
    }


    @PostMapping("/add")
    public ResponseEntity<?> saveDate(@RequestBody Attributes t) {
        if (t == null) {
            throw new org.springframework.web.server.ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Request body is required.");
        }

        // Adapt Jackson REST mapping to validation schema
        if (t.getRawDataType() == null && t.getDataType() != null) {
            t.setRawDataType(t.getDataType().getValue());
        }
        if (t.getRawNullable() == null) {
            t.setRawNullable(t.getIsNullable() == 1 ? "Yes" : "No");
        }

        // Leverage efficient single-record existence check for fast REST validation
        java.util.Set<String> existingNames = new java.util.HashSet<>();
        if (t.getAttributeName() != null && dataService != null) {
            String attrName = t.getAttributeName().trim();
            // Multi-tenant safe native MongoDB Regex Query ensuring 100% case-insensitivity
            org.springframework.data.mongodb.core.query.Query query = 
                    new org.springframework.data.mongodb.core.query.Query(
                            org.springframework.data.mongodb.core.query.Criteria.where("attributeName")
                                    .regex("^" + java.util.regex.Pattern.quote(attrName) + "$", "i")
                    );
            Collection<Attributes> existingList = dataService.fetchData(query, Attributes.class);
        if (existingList != null && !existingList.isEmpty()) {
            for (Attributes existing : existingList) {
                // Ensure robust string comparison for IDs
                String existingId = existing.getId() != null ? existing.getId().toString() : null;
                String requestId = t.getId() != null ? t.getId().toString() : null;
                
                log.debug("Checking duplicate: requestName={}, requestId={}, existingId={}", attrName, requestId, existingId);
                
                if (requestId == null || !requestId.equals(existingId)) {
                    existingNames.add(attrName.toUpperCase());
                    log.info("Duplicate attribute name found: {} (Existing ID: {}, Request ID: {})", attrName, existingId, requestId);
                    break;
                }
            }
        }
        }

        // Execute full business validation matrix
        java.util.List<com.reserv.dataloader.batch.exception.ItemValidationException.ValidationError> errors = 
                validator.validate(t, existingNames);

        // Filter errors and format exceptions 
        boolean hasErrors = errors.stream().anyMatch(e -> "ERROR".equals(e.getSeverity()));
        if (hasErrors) {
            String message = errors.stream()
                    .filter(e -> "ERROR".equals(e.getSeverity()))
                    .map(e -> "[" + e.getErrorCode() + "] " + e.getMessage())
                    .collect(java.util.stream.Collectors.joining(" | "));
            return ResponseEntity.badRequest().body(message);

        }

        dataService.save(t);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/get/all")
    public ResponseEntity<Collection<Attributes>> getAllAttributes() {
        try {
            Collection<Attributes> attributes = dataService.fetchAllData(Attributes.class);
            return new ResponseEntity<>(attributes, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/all/options")
    public ResponseEntity<Collection<Option>> getAllAttributeOptions() {
        try {
            Collection<Option> attributes = attributeService.getAttributeNameOptions();
            return new ResponseEntity<>(attributes, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/get/isreclassable/attributes")
    public ResponseEntity<Collection<Attributes>> getIsReclassableAttributes() {
        try {
            Collection<Attributes> attributes = this.attributeService.getReclassableAttributes();
            return new ResponseEntity<>(attributes, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
