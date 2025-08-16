package com.fyntrac.model.controller;

import com.fyntrac.common.model.ExcelModelProcessor;
import com.fyntrac.model.service.ModelExecutionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@Slf4j
@RequestMapping("/api/model")
public class ModelExecutionController {

    private final ModelExecutionService modelExecutionService;

    @Autowired
    public ModelExecutionController(ModelExecutionService modelExecutionService) {
        this.modelExecutionService = modelExecutionService;
    }

    @PostMapping("/execute")
    public void saveData() throws IOException {
        // Sample Input Data
        List<Map<String, Object>> iTransactionData = List.of(
                Map.of("TransactionName", "ORIGINATION-UPB", "Amount", 5000, "TransactionDate", "2024-01-08", "InstrumentId", "ABC", "AttributeId", 101),
                Map.of("TransactionName", "ORIGINATION-FEE", "Amount", 1200, "TransactionDate", "2024-01-08", "InstrumentId", "ABC", "AttributeId", 101)
        );

        List<Map<String, Object>> iAggregationData = List.of(
                Map.of("MetricName", "Equity", "InstrumentId", "ABC", "AccountingPeriod", "2024-Q1", "Balance.BeginningBalance", 1000000, "Activity", 50000, "EndingBalance", 1050000)
        );

        List<Map<String, Object>> iInstrumentAttributeData = List.of(
                Map.of("Type", "Bond", "EffectiveDate", "2023-12-01", "AttributeID", 201, "AmortizationMethod", "Straight", "NoteRate", 5.0, "Term", 10)
        );

        String home = System.getProperty("user.home"); // Get the home directory
        File excelFile = new File(home + "/Documents/Test/ModelExcel.xlsx");
        File outputFile = new File(home + "/Workspace/test/model/processed_output.xlsx");
        if(outputFile.exists()){
            boolean isDeleted = outputFile.delete();
        }
        ExcelModelProcessor.processExcel(excelFile, iTransactionData, iAggregationData, iInstrumentAttributeData);
    }
}
