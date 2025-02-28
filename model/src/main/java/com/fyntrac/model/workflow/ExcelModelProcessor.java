package com.fyntrac.model.workflow;

import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.model.utils.ExcelUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class ExcelModelProcessor {

    public static void processExcel(File inputFile,
                                    List<Map<String, Object>> iTransactionData,
                                    List<Map<String, Object>> iAggregationData,
                                    List<Map<String, Object>> iInstrumentAttributeData) throws IOException {

        try (FileInputStream fis = new FileInputStream(inputFile);
             Workbook workbook = new XSSFWorkbook(fis)) {
            // processExcel("", workbook, iTransactionData, iAggregationData, iInstrumentAttributeData);
        }
    }
    public static void processExcel(InstrumentAttribute instrumentAttribute
                                    , Date executionDate
                                    , AccountingPeriod accountingPeriod
                                    , Workbook workbook
                                    , List<Map<String, Object>> iTransactionData
                                    , List<Map<String, Object>> iMetricData
                                    , List<Map<String, Object>> iInstrumentAttributeData) throws IOException {

        if (workbook != null) {

            // Write Input Data
            writeSheetData(workbook, "i_transaction", iTransactionData);
            writeSheetData(workbook, "i_metric", iMetricData);
            writeSheetData(workbook, "i_instrumentattribute", iInstrumentAttributeData);

            // Evaluate formulas (if applicable)
            FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
            evaluator.evaluateAll();

            // Read Output Data
            List<Map<String, Object>> oTransactionData = readSheetData(workbook, "o_transaction");
            List<Map<String, Object>> oInstrumentAttributeData = readSheetData(workbook, "o_instrumentattribute");

            // Save the modified file
            String fileName = "processed_output" + instrumentAttribute.getInstrumentId() + ".xlsx";
            try (FileOutputStream fos = new FileOutputStream(fileName)) {
                workbook.write(fos);
            }

            log.info("Processing completed successfully!");
        }
    }

    private static void writeSheetData(Workbook workbook, String sheetName, List<Map<String, Object>> data) throws IOException {
        ExcelUtil.mapJsonToExcel(data, workbook, sheetName);
    }

    private static List<Map<String, Object>> readSheetData(Workbook workbook, String sheetName) {
        Sheet sheet = workbook.getSheet(sheetName);
        List<Map<String, Object>> dataList = new ArrayList<>();

        if (sheet == null) {
            log.error("Sheet not found: " + sheetName);
            return dataList;
        }

        Row headerRow = sheet.getRow(0);
        if (headerRow == null) return dataList;

        int columnCount = headerRow.getPhysicalNumberOfCells();

        for (int i = 1; i <= sheet.getLastRowNum(); i++) {
            Row row = sheet.getRow(i);
            if (row == null) continue;

            Map<String, Object> rowData = new HashMap<>();
            for (int j = 0; j < columnCount; j++) {
                Cell cell = row.getCell(j, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                rowData.put(headerRow.getCell(j).getStringCellValue(), getCellValue(cell));
            }
            dataList.add(rowData);
        }
        return dataList;
    }

    private static Object getCellValue(Cell cell) {
        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue();
            case NUMERIC -> (DateUtil.isCellDateFormatted(cell) ? getDateValue(cell) : cell.getNumericCellValue());
            case BOOLEAN -> cell.getBooleanCellValue();
            case FORMULA -> cell.getCellFormula();
            default -> "";
        };
    }

    private static String getDateValue(Cell cell) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date date = cell.getDateCellValue();
        return  df.format(date);
    }

}
