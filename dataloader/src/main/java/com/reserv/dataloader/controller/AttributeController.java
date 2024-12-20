package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.Attributes;
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

    @Autowired
    public AttributeController(DataService dataService, AttributeService attributeService) {
        this.dataService = dataService;
        this.attributeService = attributeService;
    }


    @PostMapping("/add")
    public void saveDate(@RequestBody Attributes t) {
        dataService.save(t);
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
