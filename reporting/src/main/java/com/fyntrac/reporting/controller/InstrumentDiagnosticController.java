package com.fyntrac.reporting.controller;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.reporting.service.InstrumentDiagnosticService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

@RestController
@RequestMapping("/api/reporting/diagnostic")
@Slf4j
public class InstrumentDiagnosticController {

    private final InstrumentDiagnosticService instrumentDiagnosticService;

    public InstrumentDiagnosticController(InstrumentDiagnosticService instrumentDiagnosticService){
        this.instrumentDiagnosticService = instrumentDiagnosticService;
    }

    @PostMapping("/generate")
    public ResponseEntity<Records.DiagnosticReportDataRecord> executeJournalEntryReport(@RequestBody Records.DiagnosticReportRequestRecord diagnosticRequest) throws Throwable {
        try {
         Records.DiagnosticReportDataRecord reportDataRecord =  instrumentDiagnosticService.generateDiagnostic(diagnosticRequest);
            return new ResponseEntity<>(reportDataRecord, HttpStatus.OK);
        }catch (Exception e) {
            // Log the exception for debugging purposes
            log.error(e.getLocalizedMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/download")
    public ResponseEntity<byte[]> downloadReport(@RequestBody Records.DiagnosticReportRequestRecord diagnosticRequest) throws IOException {
        Object cached = this.instrumentDiagnosticService.getDiagnosticFile(diagnosticRequest);
        if (cached == null || !(cached instanceof File file)) {
            return ResponseEntity.notFound().build();
        }

        byte[] fileContent = Files.readAllBytes(file.toPath());

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + file.getName() + "\"")
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(fileContent);
    }

}
