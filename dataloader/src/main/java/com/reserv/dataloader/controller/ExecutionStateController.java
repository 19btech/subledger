package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.service.ExecutionStateService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

@RestController
@RequestMapping("/api/dataloader/execution/state")
@Slf4j
public class ExecutionStateController {

    private final ExecutionStateService executionStateService;
    public ExecutionStateController(ExecutionStateService executionStateService) {
        this.executionStateService = executionStateService;
    }
    @GetMapping("/get/latest")
    public ResponseEntity<com.fyntrac.common.entity.ExecutionState> getExecutionState() {
        try {
            com.fyntrac.common.entity.ExecutionState executionState = this.executionStateService.getExecutionState();
            return new ResponseEntity<>(executionState, HttpStatus.OK);
        } catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
