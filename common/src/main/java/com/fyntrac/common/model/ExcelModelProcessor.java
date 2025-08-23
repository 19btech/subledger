package com.fyntrac.common.model;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.utils.ExcelModelUtil;
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

    public static Records.ModelOutputData processExcel (String intrumentId, List<Records.InstrumentAttributeModelRecord> instrumentAttribute
            , Date executionDate
            , AccountingPeriod accountingPeriod
            , Workbook workbook
            , List<Map<String, Object>> iTransactionData
            , List<Map<String, Object>> iMetricData
            , List<Map<String, Object>> iInstrumentAttributeData
            , List<Map<String, Object>> iExecutionDate
            , boolean createModelOutputFile) throws IOException {

        Workbook generatedWorkbook = generateOutput(intrumentId,
                instrumentAttribute,
                executionDate,
                accountingPeriod,
                workbook,
                iTransactionData,
                iMetricData,
                iInstrumentAttributeData,
                iExecutionDate);

        List<Map<String, Object>> oTransactionData = new ArrayList<>(0);
        List<Map<String, Object>> oInstrumentAttributeData = new ArrayList<>(0);

        // Read Output Data
        oTransactionData = readSheetData(generatedWorkbook, "o_transaction");
        oInstrumentAttributeData = readSheetData(generatedWorkbook, "o_instrumentattribute");


        if(createModelOutputFile) {
            // Save the modified file
            String postingDate = String.format("%1$tm-%1$td-%1$tY", executionDate);
            String fileName = String.format("processed-output-%s-%s.xlsx" ,intrumentId, postingDate);
            try (FileOutputStream fos = new FileOutputStream(fileName)) {
                workbook.write(fos);
            }
        }

        return RecordFactory.createModelOutputData(oTransactionData, oInstrumentAttributeData);
    }
    public static Workbook generateOutput(String intrumentId, List<Records.InstrumentAttributeModelRecord> instrumentAttribute
            , Date executionDate
            , AccountingPeriod accountingPeriod
            , Workbook workbook
            , List<Map<String, Object>> iTransactionData
            , List<Map<String, Object>> iMetricData
            , List<Map<String, Object>> iInstrumentAttributeData
            , List<Map<String, Object>> iExecutionDate) throws IOException {


        if (workbook != null) {

            // Write Input Data
            // writeSheetData(workbook, "i_transaction", iTransactionData);
            ExcelModelUtil.fillExcelSheetByAttributeIdAndTransactionTypeOrOrder(iTransactionData, workbook, "i_transaction");
            // ExcelUtil.fillExcelWithTransactionData(workbook,iTransactionData,"i_transaction");
            // writeSheetData(workbook, "i_metric", iMetricData);
            ExcelModelUtil.fillExcelSheetByAttributeIdAndMetricNameOrOrder(iMetricData, workbook, "i_metric");
            //writeSheetData(workbook, "i_instrumentattribute", iInstrumentAttributeData);
            ExcelModelUtil.fillExcelSheetByAttributeIdOrOrder(iInstrumentAttributeData, workbook, "i_instrumentattribute");
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
                log.error("Error during workbook evaluation: " + e.getMessage());
            }
            log.info("Processing completed successfully!");
        }

        return workbook;
    }

    private static void writeSheetData(Workbook workbook, String sheetName, List<Map<String, Object>> data) throws IOException {
        // ExcelUtil.mapJsonToExcel(data, workbook, sheetName);
        ExcelModelUtil.mapJsonToExcel(data, workbook, sheetName);
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
                String fieldName = headerRow.getCell(j).getStringCellValue();
                rowData.put(fieldName.toUpperCase(), getCellValue(cell, evaluator));
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
            case NUMERIC -> {
                if (DateUtil.isCellDateFormatted(cell)) {
                    String formatString = cell.getCellStyle().getDataFormatString();

                    // Normalize format string to lowercase for comparison
                    if (formatString != null) {
                        String normalizedFormat = formatString.toLowerCase();

                        if (normalizedFormat.contains("MM/dd/yyyy")) {
                            // Return date as formatted string instead of Date object or numeric
                            Date date = cell.getDateCellValue();
                            String formattedDate = new SimpleDateFormat(formatString).format(date);
                            yield formattedDate;
                        }
                    }

                    // Default: return as java.util.Date object
                    yield getDateValue(cell);
                } else {
                    yield cell.getNumericCellValue();
                }
            }
            case BOOLEAN -> cell.getBooleanCellValue();
            case FORMULA -> evaluateFormulaCell(cell, evaluator);
            default -> "";
        };
    }


    private static Object evaluateFormulaCell(Cell cell, FormulaEvaluator evaluator) {
        if (evaluator == null) {
            return cell.getCellFormula(); // Fallback to formula string
        }

        // Evaluate the formula result
        CellValue cellValue = evaluator.evaluate(cell);
        if (cellValue == null) {
            return ""; // In case evaluation failed
        }

        // Check if the result is a date (still treated as NUMERIC in Excel)
        if (cellValue.getCellType() == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
            Date date = cell.getDateCellValue();
            return new SimpleDateFormat("MM/dd/yyyy").format(date); // Or return Date object
        }

        return switch (cellValue.getCellType()) {
            case STRING -> cellValue.getStringValue();
            case NUMERIC -> cellValue.getNumberValue();
            case BOOLEAN -> cellValue.getBooleanValue();
            case ERROR -> "ERROR"; // Could return cellValue.getErrorValue() if needed
            default -> "";
        };
    }


    private static String getDateValue(Cell cell) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date date = cell.getDateCellValue();
        return  df.format(date);
    }

}
