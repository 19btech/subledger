package com.reserv.dataloader.controller;

import com.reserv.dataloader.entity.AccountTypes;
import com.reserv.dataloader.entity.ActivityLog;
import com.reserv.dataloader.service.ActivityLogService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

@Slf4j
@RestController
@RequestMapping("/api/dataloader/activitylog")
public class ActivityLogController {

    @Autowired
    ActivityLogService activityLogService;

    @GetMapping("/get/recent/loads")
    public ResponseEntity<Collection<ActivityLog>> getAllAggregates() {
        try {
            Collection<ActivityLog> logs = activityLogService.getRecentLoad();
            return new ResponseEntity<>(logs, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

}
