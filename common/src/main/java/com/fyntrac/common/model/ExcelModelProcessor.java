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
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class ExcelModelProcessor {

    private static final String DATE_FORMAT = "MM/dd/yyyy";
    private static final String DATE_TIME_FORMAT = "MM/dd/yyyy HH:mm:ss";

    public static void processExcel(File inputFile,
                                    List<Map<String, Object>> iTransactionData,
                                    List<Map<String, Object>> iAggregationData,
                                    List<Map<String, Object>> iInstrumentAttributeData) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputFile);
             Workbook workbook = new XSSFWorkbook(fis)) {
            // Process workbook if needed
        }
    }

    public static Records.ModelOutputData processExcel(String instrumentId,
                                                       List<Records.InstrumentAttributeModelRecord> instrumentAttribute,
                                                       Date executionDate,
                                                       AccountingPeriod accountingPeriod,
                                                       Workbook workbook,
                                                       List<Map<String, Object>> iTransactionData,
                                                       List<Map<String, Object>> iMetricData,
                                                       List<Map<String, Object>> iInstrumentAttributeData,
                                                       List<Map<String, Object>> iExecutionDate,
                                                       boolean createModelOutputFile) throws IOException {

        Workbook generatedWorkbook = generateOutput(instrumentId,
                instrumentAttribute,
                executionDate,
                accountingPeriod,
                workbook,
                iTransactionData,
                iMetricData,
                iInstrumentAttributeData,
                iExecutionDate);

        // Read Output Data
        List<Map<String, Object>> oTransactionData = readSheetData(generatedWorkbook, "o_transaction");
        List<Map<String, Object>> oInstrumentAttributeData = readSheetData(generatedWorkbook, "o_instrumentattribute");

        if (createModelOutputFile) {
            saveOutputFile(generatedWorkbook, instrumentId, executionDate);
        }

        return RecordFactory.createModelOutputData(oTransactionData, oInstrumentAttributeData);
    }

    public static Workbook generateOutput(String instrumentId,
                                          List<Records.InstrumentAttributeModelRecord> instrumentAttribute,
                                          Date executionDate,
                                          AccountingPeriod accountingPeriod,
                                          Workbook workbook,
                                          List<Map<String, Object>> iTransactionData,
                                          List<Map<String, Object>> iMetricData,
                                          List<Map<String, Object>> iInstrumentAttributeData,
                                          List<Map<String, Object>> iExecutionDate) throws IOException {

        if (workbook == null) {
            throw new IllegalArgumentException("Workbook cannot be null");
        }

        // Write Input Data
        ExcelModelUtil.fillExcelSheetByAttributeIdAndTransactionTypeOrOrder(iTransactionData, workbook, "i_transaction");
        ExcelModelUtil.fillExcelSheetByAttributeIdAndMetricNameOrOrder(iMetricData, workbook, "i_metric");
        ExcelModelUtil.fillExcelSheetByAttributeIdOrOrder(iInstrumentAttributeData, workbook, "i_instrumentattribute");
        writeSheetData(workbook, "i_executiondate", iExecutionDate);

        // Evaluate formulas
        evaluateFormulas(workbook);

        log.info("Processing completed successfully for instrument: {}", instrumentId);
        return workbook;
    }

    public static void evaluateFormulas(Workbook workbook) {
        FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();

        try {
            evaluator.evaluateAll();
        } catch (NotImplementedException e) {
            log.warn("Bulk evaluation failed, falling back to cell-by-cell evaluation");
            evaluateFormulasCellByCell(workbook, evaluator);
        } catch (Exception e) {
            log.error("Error during workbook evaluation: {}", e.getMessage());
            evaluateFormulasCellByCell(workbook, evaluator);
        }

        // 🔥 Final strong evaluation pass for VLOOKUP & problematic formulas
        forceEvaluateFormulasInCell(workbook, evaluator);
    }

    private static void evaluateFormulasCellByCell(Workbook workbook, FormulaEvaluator evaluator) {
        for (Sheet sheet : workbook) {
            for (Row row : sheet) {
                for (Cell cell : row) {
                    if (cell.getCellType() == CellType.FORMULA) {
                        evaluateFormulaCell(cell, evaluator);
                    }
                }
            }
        }
    }

    private static void evaluateFormulaCell(Cell cell, FormulaEvaluator evaluator) {
        try {
            evaluator.evaluateFormulaCell(cell);
        } catch (NotImplementedException e) {
            log.error("Unsupported formula in {}!{}: {}",
                    cell.getSheet().getSheetName(),
                    cell.getAddress(),
                    cell.getCellFormula());
            cell.setCellErrorValue(FormulaError.NA.getCode());
        } catch (Exception e) {
            log.error("Error evaluating formula in {}!{}: {}",
                    cell.getSheet().getSheetName(),
                    cell.getAddress(),
                    e.getMessage());
            cell.setCellErrorValue(FormulaError.VALUE.getCode());
        }
    }

    private static void forceEvaluateFormulasInCell(Workbook workbook, FormulaEvaluator evaluator) {
        // 🔥 Clearing cached results is critical!
        evaluator.clearAllCachedResultValues();

        for (Sheet sheet : workbook) {
            for (Row row : sheet) {
                for (Cell cell : row) {
                    if (cell.getCellType() == CellType.FORMULA) {
                        try {
                            evaluator.evaluateInCell(cell); // <= STRONGEST evaluation
                        } catch (Exception e) {
                            log.error("Error during in-cell evaluation in {}!{}: {}",
                                    sheet.getSheetName(),
                                    cell.getAddress(),
                                    e.getMessage());
                        }
                    }
                }
            }
        }
    }

    private static void writeSheetData(Workbook workbook, String sheetName, List<Map<String, Object>> data) throws IOException {
        ExcelModelUtil.mapJsonToExcel(data, workbook, sheetName);
    }

    private static void saveOutputFile(Workbook workbook, String instrumentId, Date executionDate) throws IOException {
        String postingDate = new SimpleDateFormat("MM-dd-yyyy").format(executionDate);
        String fileName = String.format("processed-output-%s-%s.xlsx", instrumentId, postingDate);

        try (FileOutputStream fos = new FileOutputStream(fileName)) {
            workbook.write(fos);
            log.info("Output file saved: {}", fileName);
        }
    }

    public static List<Map<String, Object>> readSheetData(Workbook workbook, String sheetName) {
        Sheet sheet = workbook.getSheet(sheetName);
        List<Map<String, Object>> dataList = new ArrayList<>();

        if (sheet == null) {
            log.warn("Sheet not found: {}", sheetName);
            return dataList;
        }

        Row headerRow = sheet.getRow(0);
        if (headerRow == null) {
            log.warn("No header row found in sheet: {}", sheetName);
            return dataList;
        }

        int columnCount = headerRow.getPhysicalNumberOfCells();
        FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();

        for (int i = 1; i <= sheet.getLastRowNum(); i++) {
            Row row = sheet.getRow(i);
            if (row == null) continue;

            Map<String, Object> rowData = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (int j = 0; j < columnCount; j++) {
                Cell headerCell = headerRow.getCell(j);
                if (headerCell == null) continue;

                String fieldName = headerCell.getStringCellValue();
                Cell dataCell = row.getCell(j, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                Object cellValue = getCellValue(dataCell, evaluator);
                rowData.put(fieldName, cellValue);
            }
            dataList.add(rowData);
        }

        log.debug("Read {} rows from sheet: {}", dataList.size(), sheetName);
        return dataList;
    }

    private static Object getCellValue(Cell cell, FormulaEvaluator evaluator) {
        if (cell == null) {
            return "";
        }

        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue().trim();
            case NUMERIC -> DateUtil.isCellDateFormatted(cell) ? getFormattedDateValue(cell) : getNumericValue(cell);
            case BOOLEAN -> cell.getBooleanCellValue();
            case FORMULA -> evaluateFormulaCellValue(cell, evaluator);
            default -> "";
        };
    }

    private static Object getFormattedDateValue(Cell cell) {
        try {
            Date date = cell.getDateCellValue();
            if (date == null) {
                return "";
            }

            String formatString = cell.getCellStyle().getDataFormatString();
            short dataFormat = cell.getCellStyle().getDataFormat();

            if (formatString == null || formatString.isEmpty() || "General".equals(formatString)) {
                return detectAndFormatDate(date);
            }

            if (isBuiltInDateFormat(dataFormat)) {
                return formatWithBuiltInFormat(date, dataFormat);
            }

            return formatWithCustomFormat(date, formatString);

        } catch (Exception e) {
            log.warn("Error parsing date cell: {}", e.getMessage());
            return cell.getDateCellValue();
        }
    }

    private static String detectAndFormatDate(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        boolean hasTime = cal.get(Calendar.HOUR_OF_DAY) != 0 ||
                cal.get(Calendar.MINUTE) != 0 ||
                cal.get(Calendar.SECOND) != 0;

        return hasTime ?
                new SimpleDateFormat(DATE_TIME_FORMAT).format(date) :
                new SimpleDateFormat(DATE_FORMAT).format(date);
    }

    private static boolean isBuiltInDateFormat(short dataFormat) {
        Set<Short> dateFormats = Set.of(
                (short) 14, (short) 15, (short) 16, (short) 17,
                (short) 18, (short) 19, (short) 20, (short) 21,
                (short) 22, (short) 27, (short) 30, (short) 36,
                (short) 50, (short) 57, (short) 58
        );
        return dateFormats.contains(dataFormat);
    }

    private static String formatWithBuiltInFormat(Date date, short dataFormat) {
        Map<Short, String> formatMap = Map.ofEntries(
                Map.entry((short) 14, "MM/dd/yyyy"),
                Map.entry((short) 15, "dd-MMM-yy"),
                Map.entry((short) 16, "dd-MMM"),
                Map.entry((short) 17, "MMM-yy"),
                Map.entry((short) 18, "h:mm a"),
                Map.entry((short) 19, "h:mm:ss a"),
                Map.entry((short) 20, "h:mm"),
                Map.entry((short) 21, "h:mm:ss"),
                Map.entry((short) 22, "MM/dd/yyyy h:mm"),
                Map.entry((short) 27, "yyyy年M月d日"),
                Map.entry((short) 30, "M/d/yy"),
                Map.entry((short) 36, "yyyy-MM-dd"),
                Map.entry((short) 50, "yyyy-MM-dd"),
                Map.entry((short) 57, "yyyy-MM-dd"),
                Map.entry((short) 58, "MM/dd/yyyy")
        );

        String pattern = formatMap.getOrDefault(dataFormat, DATE_FORMAT);
        return new SimpleDateFormat(pattern).format(date);
    }

    private static String formatWithCustomFormat(Date date, String formatString) {
        try {
            String normalizedFormat = normalizeExcelFormat(formatString);
            return new SimpleDateFormat(normalizedFormat).format(date);
        } catch (Exception e) {
            log.warn("Failed to parse custom format '{}', using default", formatString);
            return new SimpleDateFormat(DATE_FORMAT).format(date);
        }
    }

    private static String normalizeExcelFormat(String excelFormat) {
        if (excelFormat == null) return DATE_FORMAT;

        String format = excelFormat.toLowerCase()
                .replace("mmmm", "MMMM")
                .replace("mmm", "MMM")
                .replace("dddd", "EEEE")
                .replace("ddd", "EEE")
                .replace("am/pm", "a")
                .replaceAll("\\[.*?\\]", "")
                .replaceAll(";.*", "")
                .trim();

        return format.isEmpty() ? DATE_FORMAT : format;
    }

    private static Object getNumericValue(Cell cell) {
        double value = cell.getNumericCellValue();

        if (value == Math.floor(value) && !Double.isInfinite(value)) {
            long longValue = (long) value;
            return (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) ?
                    (int) longValue : longValue;
        }
        return value;
    }

    private static Object evaluateFormulaCellValue(Cell cell, FormulaEvaluator evaluator) {
        if (evaluator == null) {
            return cell.getCellFormula();
        }

        try {
            CellValue cellValue = evaluator.evaluate(cell);
            if (cellValue == null) return "";

            return switch (cellValue.getCellType()) {
                case STRING -> cellValue.getStringValue();
                case NUMERIC -> {
                    if (DateUtil.isCellDateFormatted(cell)) {
                        Date date = cell.getDateCellValue();
                        yield new SimpleDateFormat(DATE_FORMAT).format(date);
                    }
                    yield cellValue.getNumberValue();
                }
                case BOOLEAN -> cellValue.getBooleanValue();
                case ERROR -> "#ERROR: " + cellValue.getErrorValue();
                default -> "";
            };
        } catch (Exception e) {
            log.warn("Error evaluating formula cell: {}", e.getMessage());
            return cell.getCellFormula();
        }
    }
}