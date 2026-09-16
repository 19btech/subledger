package com.reserv.dataloader.controller;

import com.reserv.dataloader.entity.UploadStatus;
import com.reserv.dataloader.service.UploadStatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Polling endpoint for the async upload flow (Stage 0, docs/K8S_SCALING_STRATEGY.md) —
 * lets a client that received a 202 + uploadId from
 * {@code POST /api/dataloader/accounting/rule/upload-async} find out when processing finishes,
 * instead of the old model where the HTTP response itself only returned once fully done.
 */
@Slf4j
@RestController
@RequestMapping("/api/dataloader/upload")
public class UploadStatusController {

    @Autowired
    UploadStatusService uploadStatusService;

    @GetMapping("/status/{uploadId}")
    public ResponseEntity<UploadStatus> getStatus(@PathVariable long uploadId) {
        return uploadStatusService.getStatus(uploadId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }
}
