package com.fyntrac.model.workflow;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.model.utils.ExcelUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.formula.eval.NotImplementedException;
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
    public static Records.ModelOutputData processExcel(InstrumentAttribute instrumentAttribute
                                    , Date executionDate
                                    , AccountingPeriod accountingPeriod
                                    , Workbook workbook
                                    , List<Map<String, Object>> iTransactionData
                                    , List<Map<String, Object>> iMetricData
                                    , List<Map<String, Object>> iInstrumentAttributeData
                                    , List<Map<String, Object>> iExecutionDate) throws IOException {

        List<Map<String, Object>> oTransactionData = new ArrayList<>(0);
        List<Map<String, Object>> oInstrumentAttributeData = new ArrayList<>(0);

        if (workbook != null) {

            // Write Input Data
            writeSheetData(workbook, "i_transaction", iTransactionData);
            writeSheetData(workbook, "i_metric", iMetricData);
            writeSheetData(workbook, "i_instrumentattribute", iInstrumentAttributeData);
            writeSheetData(workbook, "i_executiondate", iExecutionDate);
            // Evaluate formulas (if applicable)
            FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
            try {
                // First try bulk evaluation for better performance
                evaluator.evaluateAll();
            } catch (NotImplementedException bulkEvalException) {
                // If bulk evaluation fails, fall back to cell-by-cell evaluation
                System.err.println("Bulk evaluation failed, falling back to cell-by-cell evaluation");

                for (Sheet sheet : workbook) {
                    for (Row row : sheet) {
                        for (Cell cell : row) {
                            if (cell.getCellType() == CellType.FORMULA) {
                                try {
                                    // Evaluate individual cell
                                    evaluator.evaluateFormulaCell(cell);
                                } catch (NotImplementedException e) {
                                    // Handle unsupported formula
                                    log.error("Unsupported formula in %s!%s: %s%n",
                                            sheet.getSheetName(),
                                            cell.getAddress(),
                                            cell.getCellFormula());

                                    // Set cell to #N/A error value
                                    cell.setCellErrorValue(FormulaError.NA.getCode());
                                } catch (Exception e) {
                                    // Handle other potential evaluation errors
                                    log.error("Error evaluating formula in %s!%s: %s%n",
                                            sheet.getSheetName(),
                                            cell.getAddress(),
                                            e.getMessage());
                                    cell.setCellErrorValue(FormulaError.VALUE.getCode());
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                // Handle other workbook-level evaluation errors
                System.err.println("Error during workbook evaluation: " + e.getMessage());
            }
            // Read Output Data
            oTransactionData = readSheetData(workbook, "o_transaction");
            oInstrumentAttributeData = readSheetData(workbook, "o_instrumentattribute");


            // Save the modified file
            String fileName = "processed_output" + instrumentAttribute.getInstrumentId() + ".xlsx";
            try (FileOutputStream fos = new FileOutputStream(fileName)) {
                workbook.write(fos);
            }

            log.info("Processing completed successfully!");
        }
        return RecordFactory.createModelOutputData(oTransactionData, oInstrumentAttributeData);
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
        FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();

        for (int i = 1; i <= sheet.getLastRowNum(); i++) {
            Row row = sheet.getRow(i);
            if (row == null) continue;

            Map<String, Object> rowData = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (int j = 0; j < columnCount; j++) {
                Cell cell = row.getCell(j, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                rowData.put(headerRow.getCell(j).getStringCellValue(), getCellValue(cell, evaluator));
            }
            dataList.add(rowData);
        }
        return dataList;
    }


    private static Object getCellValue(Cell cell, FormulaEvaluator evaluator) {
        if (cell == null) {
            return ""; // Return empty string for null cells
        }

        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue();
            case NUMERIC -> (DateUtil.isCellDateFormatted(cell) ? getDateValue(cell) : cell.getNumericCellValue());
            case BOOLEAN -> cell.getBooleanCellValue();
            case FORMULA -> evaluateFormulaCell(cell, evaluator); // Evaluate the formula
            default -> "";
        };
    }

    private static Object evaluateFormulaCell(Cell cell, FormulaEvaluator evaluator) {
        if (evaluator == null) {
            return cell.getCellFormula(); // Fallback to formula string if evaluator is not provided
        }

        CellValue cellValue = evaluator.evaluate(cell); // Evaluate the formula
        return switch (cellValue.getCellType()) {
            case STRING -> cellValue.getStringValue();
            case NUMERIC -> cellValue.getNumberValue();
            case BOOLEAN -> cellValue.getBooleanValue();
            case ERROR -> "ERROR"; // Handle formula errors
            default -> "";
        };
    }

    private static String getDateValue(Cell cell) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date date = cell.getDateCellValue();
        return  df.format(date);
    }

}
