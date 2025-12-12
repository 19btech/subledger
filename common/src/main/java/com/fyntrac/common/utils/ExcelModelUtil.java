package com.fyntrac.common.utils;

import com.fyntrac.common.enums.AccountingRules;
import com.fyntrac.common.exception.ExcelFormulaCellException;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.formula.eval.NotImplementedException;
import org.apache.poi.ss.usermodel.*;
import org.bson.types.Binary;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipInputStream;


@Slf4j
public class ExcelModelUtil {

    public static CellStyle DATE_STYLE;
    public static CellStyle NUMBER_STYLE;
    public static CellStyle INT_STYLE;

    public static final DateTimeFormatter DATE_INPUT_FMT =
            DateTimeFormatter.ofPattern("MM/dd/yyyy");

    public static void preCreateStyles(Workbook workbook) {
        CreationHelper helper = workbook.getCreationHelper();

        DATE_STYLE = workbook.createCellStyle();
        DATE_STYLE.setDataFormat(helper.createDataFormat().getFormat("MM/dd/yyyy"));

        NUMBER_STYLE = workbook.createCellStyle();
        NUMBER_STYLE.setDataFormat(helper.createDataFormat().getFormat("#,##0.00"));

        INT_STYLE = workbook.createCellStyle();
        INT_STYLE.setDataFormat(helper.createDataFormat().getFormat("#,##0"));
    }

    public static File saveFileToTempFolder(String filePath, String fileName, byte[] fildData) throws Exception {

        org.apache.commons.io.FileUtils.deleteDirectory(new File(filePath));
        new File(filePath + File.separator).mkdirs();
        File uploadedFile = new File(filePath + File.separator + fileName);

        uploadedFile.createNewFile();
        FileOutputStream fos = new FileOutputStream(uploadedFile);
        fos.write(fildData);
        fos.close();

        return uploadedFile;
    }

    public File convertXlsToCSV(File file) {

        return null;
    }


    public String getHeaderOfCSVFile(File csvFile) {
        BufferedReader br = null;
        String strLine = "";
        try {
            LineNumberReader reader = new LineNumberReader(new InputStreamReader(new FileInputStream(csvFile.getAbsoluteFile()), "UTF-8"));
            if (((strLine = reader.readLine()) != null) && reader.getLineNumber() == 1) {
                return strLine;
            }

            reader.close();

        } catch (FileNotFoundException e) {
           log.error("File not found");
        } catch (IOException e) {
           log.error("Unable to read the file.");
        }
        return null;
    }

    public static List<String> getSheetListFromExcelFile(String fileNameWithAbsolutePath) throws Throwable {

        List<String> xlSheetList = new ArrayList<>(0);
        try {
            InputStream inp = new FileInputStream(fileNameWithAbsolutePath);
            Workbook wb = WorkbookFactory.create(inp);
            for (Sheet sheet : wb) {
                xlSheetList.add(sheet.getSheetName());
            }
        } catch (IOException ex) {
            throw ex;
        }

        return xlSheetList;
    }

    public static Set<String> getSheetNameList(String fileNameWithAbsolutePath) throws Throwable {
        Set<String> nameSet = new HashSet<>(0);
        InputStream inp = null;
        try {
            inp = new FileInputStream(fileNameWithAbsolutePath);
            Workbook wb = WorkbookFactory.create(inp);
            for (Sheet sheet : wb) {
                nameSet.add(sheet.getSheetName().toUpperCase());
            }
        } catch (FileNotFoundException ex) {
            throw ex;
        } catch (UnsupportedEncodingException ex) {
            throw ex;
        } catch (IOException ex) {
            throw ex;
        } finally {
            try {
                inp.close();
            } catch (IOException ex) {
                throw ex;
            }
        }
        return nameSet;
    }

    public static void convertExcelToCSV(String fileNameWithAbsolutePath,
                                         String outPutFilePath,
                                         Long activityUploadId) throws Throwable {
        InputStream inp = null;
        try {
            inp = new FileInputStream(fileNameWithAbsolutePath);
            Workbook wb = WorkbookFactory.create(inp);
            FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();
            for (Sheet sheet : wb) {

                if (!AccountingRules.isValid(sheet.getSheetName().toLowerCase() + ".csv")) {
//                    throw new InvalidExcelSheetNameException("Invalid SheetName["
//                            + sheet.getSheetName() + "] of file[" + "]");
                    continue;
                }

                String sheetName = sheet.getSheetName().toLowerCase();
                String outputFile = outPutFilePath + File.separator + sheetName + ".csv";
                Path outputFilePath = Path.of(outputFile);

                if (!Files.exists(outputFilePath.getParent())) {
                    Files.createDirectories(outputFilePath.getParent());
                }
//                Files.deleteIfExists(outputFilePath);
//                Files.createFile(outputFilePath);
                BufferedWriter writer = new BufferedWriter(new FileWriter(outPutFilePath + File.separator + sheetName + ".csv", true));
                boolean isFirstRow = Boolean.TRUE;
                int numOfCells = 0;

                Iterator<Row> rowIterator = sheet.rowIterator();
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    if (isRowEmpty(row)) {
                        continue;
                    }
                    boolean isFirstCell = Boolean.TRUE;
                    if (isFirstRow) {
                        numOfCells = row.getPhysicalNumberOfCells();
                    }

                    for (int cellIndex = 0; cellIndex < numOfCells; cellIndex++) {
                        Cell cell = row.getCell(cellIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                        if (isFirstRow && activityUploadId != null) {
                            isFirstRow = Boolean.FALSE;
                            isFirstCell = Boolean.FALSE;
                            writer.append("\"activityUploadId\"".toUpperCase());
                            writer.append(',');
                        } else if (isFirstCell) {
                            isFirstCell = Boolean.FALSE;
                            writer.append(activityUploadId + "");
                            writer.append(',');
                        } else {
                            writer.append(',');
                        }
                        switch (cell.getCellType()) {
                            case BOOLEAN:
                                writer.append(cell.getBooleanCellValue() + "");
                                break;
                            case NUMERIC:
                                writer.append(((org.apache.poi.ss.usermodel.DateUtil.isCellDateFormatted(cell)) ? getDateValue(cell) : getNumericValue(cell)) + "");
                                break;
                            case STRING:
                                writer.append("\"" + String.valueOf(cell.getStringCellValue()).toUpperCase()
                                        + "\"");
                                break;
                            case BLANK:
                                writer.append("\"" + "\"");
                                break;
                            case FORMULA:
                                setFormulaCellValue(cell, writer, evaluator, cellIndex);
                                break;
                            case _NONE:
                                writer.append("\"" + "\"");
                                break;
                            default:
                                writer.append(cell + "");

                        }
                    }
                    writer.append("\n");
                    isFirstRow = Boolean.FALSE;
                }
                writer.flush();
                writer.close();
            }
        } catch (FileNotFoundException ex) {
            throw ex;
        } catch (UnsupportedEncodingException ex) {
            throw ex;
        } catch (IOException ex) {
            throw ex;
        } finally {
            try {
                inp.close();
            } catch (IOException ex) {
                throw ex;
            }
        }
    }

    protected static String getDateValue(Cell cell) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date date = cell.getDateCellValue();
        return df.format(date);
    }

    protected static String getNumericValue(Cell cell) {
        Double value = 0.0d;
        if (cell.getCellStyle().getDataFormatString().contains("%")) {
            // Detect Percent Values
            value = cell.getNumericCellValue() / 100;
            System.out.println("Percent value found = " + value.toString() + "%");
        } else {
            value = cell.getNumericCellValue();
            System.out.println("Non percent value found = " + value.toString());
        }
        return value.toString();
    }

    protected static void setFormulaCellValue(Cell cell, BufferedWriter data, FormulaEvaluator evaluator,
                                              int cellIndex) throws Exception {
        try {
            CellValue cellValue = evaluator.evaluate(cell);
            switch (cellValue.getCellType()) {
                case BOOLEAN:
                    data.append(cellValue.getBooleanValue() + "");
                    break;
                case NUMERIC:
                    // Check if the numeric value is a date
                    if (org.apache.poi.ss.usermodel.DateUtil.isCellDateFormatted(cell)) {
                        Date date = cell.getDateCellValue();
                        // Format the date as needed, e.g., "yyyy-MM-dd"
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        data.append("\"" + dateFormat.format(date) + "\"");
                    } else {
                        data.append(cellValue.getNumberValue() + "");
                    }
                    break;
                default:
                    data.append("\"" + cellValue.getStringValue() + "\"");
            }
        } catch (Exception exp) {
            exp.printStackTrace();
            throw new ExcelFormulaCellException(exp.getLocalizedMessage());
        }
    }

    static String getColSeparator(int colIndex, int lastColIndex) {
        if (colIndex == lastColIndex - 1) {
            return "";
        } else {
            return ",";
        }
    }

    public static File writeFile(String filePath, String fileName, byte[] data) throws IOException {
        File outputFile = new File(filePath + File.separator + fileName);
        try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
            outputStream.write(data);
        }

        return outputFile;
    }

    public static Set<String> getFileList(String dir) {
        return Stream.of(new File(dir).listFiles())
                .filter(file -> !file.isDirectory())
                .map(File::getName)
                .collect(Collectors.toSet());
    }

    public static String getHomePath() {
        return System.getProperty("user.home") + File.separator + "tmp";
    }

    public static boolean isRowEmpty(Row row) {
        boolean isEmpty = true;
        DataFormatter dataFormatter = new DataFormatter();
        if (row != null) {
            for (Cell cell : row) {
                if (dataFormatter.formatCellValue(cell).trim().length() > 0) {
                    isEmpty = false;
                    break;
                }
            }
        }
        return isEmpty;
    }


    protected static void extractFile(ZipInputStream zipIn, File file) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = zipIn.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }
    }

    public static boolean isZipFile(File file) {
        // Get the file name
        String fileName = file.getName();

        // Check if the file name ends with ".zip" (case-insensitive)
        return fileName.toLowerCase().endsWith(".zip");
    }


    public static boolean isExtensionMatched(String fileName, String expectedExtension) {
        // Get the file extension from the file name
        String fileExtension = getFileExtension(fileName);

        // Check if the file extension matches the expected extension
        return fileExtension != null && fileExtension.equalsIgnoreCase(expectedExtension);
    }

    protected static String getFileExtension(String fileName) {
        int lastDotIndex = fileName.lastIndexOf(".");
        if (lastDotIndex != -1 && lastDotIndex < fileName.length() - 1) {
            return fileName.substring(lastDotIndex + 1);
        }
        return null;
    }

    public static boolean moveFileToFolder(String filePath, String destinationFolderPath) {
        try {
            // Create Path objects for source file and destination folder
            Path sourcePath = Paths.get(filePath);
            Path destinationPath = Paths.get(destinationFolderPath);

            // Move the file to the destination folder
            Files.move(sourcePath, destinationPath.resolve(sourcePath.getFileName()), StandardCopyOption.REPLACE_EXISTING);

            return true; // File moved successfully
        } catch (Exception e) {
            e.printStackTrace();
            return false; // Failed to move the file
        }
    }

    public static void createDirectory(String directoryPath) {
        // Create a File object representing the directory path
        File directory = new File(directoryPath);

        // Check if the directory exists
        if (!directory.exists()) {
            // Attempt to create the directory
            boolean created = directory.mkdirs();
            if (created) {
                System.out.println("Directory created successfully: " + directoryPath);
            } else {
               log.error("Failed to create directory: " + directoryPath);
            }
        } else {
            System.out.println("Directory already exists: " + directoryPath);
        }
    }

    public static List<Path> listCsvFiles(String directoryPath, String fileExtention) {
        try (Stream<Path> walk = Files.walk(Paths.get(directoryPath))) {
            return walk.filter(Files::isRegularFile)
                    .filter(path -> path.toString().toLowerCase().endsWith(fileExtention))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
            return List.of(); // Return empty list on error
        }
    }


    // Method to convert Workbook to ByteArrayOutputStream
    public static byte[] convertWorkbookToByteArray(Workbook workbook) throws IOException {
        try (workbook; ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            workbook.write(outputStream);
            return outputStream.toByteArray(); // Return the byte array
        }
        // Ensure the workbook is closed
    }


    // Recreate Workbook from byte[]
    public static Workbook recreateWorkbookFromByteArray(byte[] data) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {
            return WorkbookFactory.create(inputStream);
        }
    }

    private static Sheet getSheetIgnoreCase(Workbook workbook, String sheetName) {
        for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
            if (workbook.getSheetAt(i).getSheetName().equalsIgnoreCase(sheetName)) {
                return workbook.getSheetAt(i);
            }
        }
        return null; // Sheet not found
    }

    public static void cleanSheet(Sheet sheet) {
        // Remove all rows except the header (first row, index 0)
        int lastRowNum = sheet.getLastRowNum();
        for (int i = lastRowNum; i > 0; i--) { // Start from last row to avoid index shifting
            Row row = sheet.getRow(i);
            if (row != null) {
                sheet.removeRow(row);
            }
        }
    }

    public static void mapJsonToExcel(
            List<Map<String, Object>> jsonData,
            Workbook workbook,
            String sheetName
    ) throws IOException {

        Sheet sheet = getSheetIgnoreCase(workbook, sheetName);

        log.info("Requested Sheet Name: {}", sheetName);
        log.info("Excel Sheet Name: {}", sheet.getSheetName());

        Row headerRow = sheet.getRow(0);
        Map<String, Integer> colMap = getColumnIndexMap(headerRow);

        Map<String, Integer> caseInsensitiveMap =
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveMap.putAll(colMap);

        cleanSheet(sheet);

        int rowNum = 1; // Start after header

        for (Map<String, Object> rowMap : jsonData) {
            Row row = sheet.createRow(rowNum++);

            Map<String, Object> caseInsensitiveRow =
                    new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            caseInsensitiveRow.putAll(rowMap);

            for (String column : caseInsensitiveRow.keySet()) {

                if (!caseInsensitiveMap.containsKey(column)) continue;

                int colIndex = caseInsensitiveMap.get(column);
                Cell cell = row.createCell(colIndex);
                Object value = caseInsensitiveRow.get(column);

                try {
                    applyCellValue(cell, value, column, workbook);
                }catch (Exception e){
                   log.error("❌ ERROR in applyCellValue()");
                   log.error("   Column Name     : " + column);
                   log.error("   Column Index    : " + colIndex);
                   log.error("   Row Number      : " + row.getRowNum());
                   log.error("   Cell Address    : " + cell.getAddress());
                   log.error("   Value           : " + value);
                   log.error("   Value Type      : " + (value != null ? value.getClass().getName() : "null"));
                   log.error("   Error Message   : " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        log.debug("Successfully mapped {} rows to '{}'", jsonData.size(), sheetName);
    }

    private static String cleanString(String s) {
        if (s == null) return "";
        return s.replace("\u200B", "")
                .replace("\ufeff", "")
                .trim();
    }

    private static boolean isValidDateString(String s) {
        try {
            LocalDate.parse(s, DATE_INPUT_FMT);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // Replace with a method to get or create styles for each workbook
    private static CellStyle getNumberStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        DataFormat format = workbook.createDataFormat();
        style.setDataFormat(format.getFormat("0.00"));
        return style;
    }

    private static CellStyle getDateStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        CreationHelper createHelper = workbook.getCreationHelper();
        style.setDataFormat(createHelper.createDataFormat().getFormat("MM/dd/yyyy"));
        return style;
    }

    // Modify your applyCellValue method to accept the workbook
    private static void applyCellValue(Cell cell, Object value, String columnName, Workbook workbook) {

        log.info(String.format("columnName[%s], value[%s], cell[%s]", columnName,
                (value == null ? "null" : value.toString()), cell.toString()));

        if (value == null) {
            cell.setBlank();
            return;
        }

        // ---------------------------------------------------------
        // ALWAYS TREAT InstrumentId and AttributeId AS STRING
        // ---------------------------------------------------------
        if ("InstrumentId".equalsIgnoreCase(columnName) ||
                "AttributeId".equalsIgnoreCase(columnName)) {
            cell.setCellValue(String.valueOf(value));
            return;
        }

        // ---------------------------------------------------------
        // STRING VALUES
        // ---------------------------------------------------------
        if (value instanceof String str) {
            str = cleanString(str);

            // Numeric string → number
            if (str.matches("-?\\d+(\\.\\d+)?")) {
                cell.setCellValue(Double.parseDouble(str));
                cell.setCellStyle(getNumberStyle(workbook)); // Use workbook-specific style
                return;
            }

            if (isValidDateString(str)) {
                try {
                    LocalDate date = LocalDate.parse(str, DATE_INPUT_FMT);
                    // Use system default timezone instead of UTC to avoid offset
                    Date dateValue = Date.from(date.atStartOfDay(ZoneId.systemDefault()).toInstant());

                    cell.setCellValue(dateValue);
                    cell.setCellStyle(getDateStyle(workbook));
                    return;
                } catch (Exception e) {
                    // Fallback if parsing fails
                    cell.setCellValue(str);
                    return;
                }
            }
            cell.setCellValue(str);
            return;
        }

        // ---------------------------------------------------------
        // DATE
        // ---------------------------------------------------------
        if (value instanceof Date date) {
            // Convert to system default timezone to avoid offset issues
            LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            Date systemDate = Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
            cell.setCellValue(systemDate);
            cell.setCellStyle(getDateStyle(workbook));
            return;
        }

        // ---------------------------------------------------------
        // BIGDECIMAL
        // ---------------------------------------------------------
        if (value instanceof BigDecimal bd) {
            cell.setCellValue(bd.doubleValue());
            cell.setCellStyle(getNumberStyle(workbook)); // Use workbook-specific style
            return;
        }

        // ---------------------------------------------------------
        // OTHER NUMBERS
        // ---------------------------------------------------------
        if (value instanceof Number n) {
            cell.setCellValue(n.doubleValue());
            cell.setCellStyle(getNumberStyle(workbook)); // Use workbook-specific style
            return;
        }

        // ---------------------------------------------------------
        // BOOLEAN
        // ---------------------------------------------------------
        if (value instanceof Boolean b) {
            cell.setCellValue(b);
            return;
        }

        // ---------------------------------------------------------
        // FALLBACK STRING
        // ---------------------------------------------------------
        cell.setCellValue(value.toString());
    }


    public static void fillExcelSheetByAttributeIdOrOrder(List<Map<String, Object>> jsonData, Workbook workbook, String sheetName) {
        Sheet sheet = getSheetIgnoreCase(workbook, sheetName);
        if (sheet == null) throw new IllegalArgumentException("Sheet '" + sheetName + "' not found.");

        // Step 1: Extract and normalize headers
        Row headerRow = sheet.getRow(0);
        if (headerRow == null) throw new IllegalArgumentException("Sheet must have a header row.");

        Map<String, Integer> columnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Cell cell : headerRow) {
            columnIndexMap.put(cell.getStringCellValue().trim().toLowerCase(), cell.getColumnIndex());
        }

        Integer attributeIdCol = columnIndexMap.get("attributeid");
        Integer transactionTypeCol = columnIndexMap.get("type");

        // Step 2: Classify template rows
        Map<String, Queue<Integer>> compositeKeyToRowMap = new LinkedHashMap<>();
        List<Integer> rowsWithoutCompositeKey = new ArrayList<>();

        for (int i = 1; i <= sheet.getLastRowNum(); i++) {
            Row row = sheet.getRow(i);
            if (row == null) continue;

            String attr = (attributeIdCol != null) ? getCellValue(row.getCell(attributeIdCol)) : "";
            String txn = (transactionTypeCol != null) ? getCellValue(row.getCell(transactionTypeCol)) : "";

            if (!attr.isEmpty() && !txn.isEmpty()) {
                String key = (attr + "_" + txn).toLowerCase();
                compositeKeyToRowMap
                        .computeIfAbsent(key, k -> new LinkedList<>())
                        .add(i);
            } else {
                rowsWithoutCompositeKey.add(i);
            }
        }


        // Step 3: Track used data rows
        Set<Integer> usedIndexes = new HashSet<>();

        // Step 4: Fill rows based on composite key match
        for (int i = 0; i < jsonData.size(); i++) {
            Map<String, Object> data = jsonData.get(i);
            Object attrObj = getIgnoreCase(data, "AttributeId");
            Object txnObj = getIgnoreCase(data, "Type");

            if (attrObj == null || txnObj == null) continue;

            String key = (attrObj.toString().trim() + "_" + txnObj.toString().trim()).toLowerCase();
            if (!compositeKeyToRowMap.containsKey(key)) {
                continue;
            }

            Integer rowIndex = compositeKeyToRowMap.get(key).poll();
            if (rowIndex == null) continue;

            data.remove("ATTRIBUTEID");
            columnIndexMap.remove("attributeid");

            data.remove("TYPE");
            columnIndexMap.remove("type");

            fillRow(sheet, sheet.getRow(rowIndex), data, columnIndexMap);
            usedIndexes.add(i);
        }

        // Step 5: Fill remaining rows by order
        int fillIndex = 0;
        for (int rowIndex : rowsWithoutCompositeKey) {
            while (fillIndex < jsonData.size() && usedIndexes.contains(fillIndex)) {
                fillIndex++;
            }
            if (fillIndex >= jsonData.size()) break;

            fillRow(sheet, sheet.getRow(rowIndex), jsonData.get(fillIndex), columnIndexMap);
            usedIndexes.add(fillIndex);
            fillIndex++;
        }
    }

    private static Date parseDate(Object obj) {
        if (obj instanceof Date) return (Date) obj;
        if (obj instanceof String s) {
            try {
                return new SimpleDateFormat("M/d/yyyy").parse(s);
            } catch (ParseException e) {
                return new Date(0); // fallback
            }
        }
        return new Date(0);
    }

    private static String getCellStringValue(Cell cell) {
        if (cell == null) return "";
        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue().trim();
            case NUMERIC -> {
                double d = cell.getNumericCellValue();
                if (d == (long) d) yield String.valueOf((long) d);
                else yield String.valueOf(d);
            }
            case BOOLEAN -> String.valueOf(cell.getBooleanCellValue());
            case FORMULA -> {
                FormulaEvaluator evaluator = cell.getSheet().getWorkbook().getCreationHelper().createFormulaEvaluator();
                CellValue evaluated = evaluator.evaluate(cell);
                yield switch (evaluated.getCellType()) {
                    case STRING -> evaluated.getStringValue();
                    case NUMERIC -> String.valueOf(evaluated.getNumberValue());
                    case BOOLEAN -> String.valueOf(evaluated.getBooleanValue());
                    default -> "";
                };
            }
            default -> "";
        };
    }

    private static String getStringValue(Cell cell) {
        if (cell == null) return "";

        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue().trim();
            case NUMERIC -> {
                if (org.apache.poi.ss.usermodel.DateUtil.isCellDateFormatted(cell)) {
                    Date date = cell.getDateCellValue();
                    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
                    yield sdf.format(date);
                } else {
                    double d = cell.getNumericCellValue();
                    if (d == (long) d) yield String.valueOf((long) d);
                    else yield String.valueOf(d);
                }
            }
            case BOOLEAN -> String.valueOf(cell.getBooleanCellValue());
            case FORMULA -> {
                FormulaEvaluator evaluator = cell.getSheet()
                        .getWorkbook()
                        .getCreationHelper()
                        .createFormulaEvaluator();
                CellValue evaluated = evaluator.evaluate(cell);
                yield switch (evaluated.getCellType()) {
                    case STRING -> evaluated.getStringValue();
                    case NUMERIC -> {
                        if (org.apache.poi.ss.usermodel.DateUtil.isCellDateFormatted(cell)) {
                            Date date = cell.getDateCellValue();
                            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
                            yield sdf.format(date);
                        } else {
                            double d = evaluated.getNumberValue();
                            if (d == (long) d) yield String.valueOf((long) d);
                            else yield String.valueOf(d);
                        }
                    }
                    case BOOLEAN -> String.valueOf(evaluated.getBooleanValue());
                    default -> "";
                };
            }
            default -> "";
        };
    }

    public static void fillExcelSheetByAttributeIdAndTransactionTypeOrOrder(List<Map<String, Object>> jsonData, Workbook workbook, String sheetName) {
        Sheet sheet = getSheetIgnoreCase(workbook, sheetName);
        if (sheet == null) throw new IllegalArgumentException("Sheet '" + sheetName + "' not found.");

        // Step 1: Extract and normalize headers
        Row headerRow = sheet.getRow(0);
        if (headerRow == null) throw new IllegalArgumentException("Sheet must have a header row.");

        Map<String, Integer> columnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Cell cell : headerRow) {
            columnIndexMap.put(cell.getStringCellValue().trim().toLowerCase(), cell.getColumnIndex());
        }

        Integer attributeIdCol = columnIndexMap.get("attributeid");
        Integer transactionTypeCol = columnIndexMap.get("transactionname");

        // Step 2: Classify template rows
        Map<String, Queue<Integer>> compositeKeyToRowMap = new LinkedHashMap<>();
        List<Integer> rowsWithoutCompositeKey = new ArrayList<>();

        for (int i = 1; i <= sheet.getLastRowNum(); i++) {
            Row row = sheet.getRow(i);
            if (row == null) continue;

            String attr = (attributeIdCol != null) ? getCellValue(row.getCell(attributeIdCol)) : "";
            String txn = (transactionTypeCol != null) ? getCellValue(row.getCell(transactionTypeCol)) : "";

            if (!attr.isEmpty() && !txn.isEmpty()) {
                String key = (attr + "_" + txn).toLowerCase();
                compositeKeyToRowMap
                        .computeIfAbsent(key, k -> new LinkedList<>())
                        .add(i);
            } else {
                rowsWithoutCompositeKey.add(i);
            }
        }


        // Step 3: Track used data rows
        Set<Integer> usedIndexes = new HashSet<>();

        // Step 4: Fill rows based on composite key match
        for (int i = 0; i < jsonData.size(); i++) {
            Map<String, Object> data = jsonData.get(i);
            Object attrObj = getIgnoreCase(data, "AttributeId");
            Object txnObj = getIgnoreCase(data, "TransactionName");

            if (attrObj == null || txnObj == null) continue;

            String key = (attrObj.toString().trim() + "_" + txnObj.toString().trim()).toLowerCase();
            Integer rowIndex = compositeKeyToRowMap.get(key).poll();
            if (rowIndex == null) continue;

            data.remove("ATTRIBUTEID");
            columnIndexMap.remove("attributeid");

            data.remove("TRANSACTIONNAME");
            columnIndexMap.remove("transactionname");

            fillRow(sheet, sheet.getRow(rowIndex), data, columnIndexMap);
            usedIndexes.add(i);
        }

        // Step 5: Fill remaining rows by order
        int fillIndex = 0;
        for (int rowIndex : rowsWithoutCompositeKey) {
            while (fillIndex < jsonData.size() && usedIndexes.contains(fillIndex)) {
                fillIndex++;
            }
            if (fillIndex >= jsonData.size()) break;

            fillRow(sheet, sheet.getRow(rowIndex), jsonData.get(fillIndex), columnIndexMap);
            usedIndexes.add(fillIndex);
            fillIndex++;
        }
    }

    public static void fillExcelSheetByAttributeIdAndMetricNameOrOrder(List<Map<String, Object>> jsonData, Workbook workbook, String sheetName) {
        Sheet sheet = getSheetIgnoreCase(workbook, sheetName);
        if (sheet == null) throw new IllegalArgumentException("Sheet '" + sheetName + "' not found.");

        // Step 1: Extract and normalize headers
        Row headerRow = sheet.getRow(0);
        if (headerRow == null) throw new IllegalArgumentException("Sheet must have a header row.");

        Map<String, Integer> columnIndexMap = new HashMap<>();
        for (Cell cell : headerRow) {
            columnIndexMap.put(cell.getStringCellValue().trim().toLowerCase(), cell.getColumnIndex());
        }

        Integer attributeIdCol = columnIndexMap.get("attributeid");
        Integer metricNameCol = columnIndexMap.get("metricname");

        // Step 2: Classify template rows
        Map<String, Queue<Integer>> compositeKeyToRowMap = new LinkedHashMap<>();
        List<Integer> rowsWithoutCompositeKey = new ArrayList<>();

        for (int i = 1; i <= sheet.getLastRowNum(); i++) {
            Row row = sheet.getRow(i);
            if (row == null) continue;

            String attr = (attributeIdCol != null) ? getCellValue(row.getCell(attributeIdCol)) : "";
            String txn = (metricNameCol != null) ? getCellValue(row.getCell(metricNameCol)) : "";

            if (!txn.isEmpty()) {
                String key = (attr + "_" + txn).toLowerCase();
                compositeKeyToRowMap
                        .computeIfAbsent(key, k -> new LinkedList<>())
                        .add(i);
            } else {
                rowsWithoutCompositeKey.add(i);
            }
        }


        // Step 3: Track used data rows
        Set<Integer> usedIndexes = new HashSet<>();

        // Step 4: Fill rows based on composite key match
        for (int i = 0; i < jsonData.size(); i++) {
            Map<String, Object> data = jsonData.get(i);
            Object attrObj = getIgnoreCase(data, "AttributeId");
            String attributeId = (attrObj == null || attrObj.toString().trim().isEmpty()) ? "" : attrObj.toString().trim();

            Object txnObj = getIgnoreCase(data, "MetricName");

            if (txnObj == null) continue;

            String key = (attributeId.trim() + "_" + txnObj.toString().trim()).toLowerCase();
            if (!compositeKeyToRowMap.containsKey(key)) {
                continue;
            }
            Integer rowIndex = compositeKeyToRowMap.get(key).poll();
            if (rowIndex == null) continue;

            data.remove("ATTRIBUTEID");
            columnIndexMap.remove("attributeid");
            data.remove("METRICNAME");
            columnIndexMap.remove("metricname");
            fillRow(sheet, sheet.getRow(rowIndex), data, columnIndexMap);
            usedIndexes.add(i);
        }

        // Step 5: Fill remaining rows by order
        int fillIndex = 0;
        for (int rowIndex : rowsWithoutCompositeKey) {
            while (fillIndex < jsonData.size() && usedIndexes.contains(fillIndex)) {
                fillIndex++;
            }
            if (fillIndex >= jsonData.size()) break;

            fillRow(sheet, sheet.getRow(rowIndex), jsonData.get(fillIndex), columnIndexMap);
            usedIndexes.add(fillIndex);
            fillIndex++;
        }
    }

    private static void fillRow(Sheet sheet, Row row, Map<String, Object> data, Map<String, Integer> headerMap) {
        if (row == null) return;

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String column = entry.getKey().toLowerCase();
            Object value = entry.getValue();

            Integer colIndex = headerMap.get(column);
            if (colIndex == null) continue;

            Cell cell = row.getCell(colIndex);
            if (cell == null) cell = row.createCell(colIndex);

            if (value instanceof String strVal) {
                String trimmed = strVal.trim();
                if (trimmed.matches("-?\\d+(\\.\\d+)?")) {
                    double parsed = Double.parseDouble(trimmed);
                    if (parsed == Math.rint(parsed)) {
                        cell.setCellValue((long) parsed);
                    } else {
                        cell.setCellValue(parsed);
                    }
                } else if (value instanceof String str && isValidDate(str)) {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
                    LocalDate localDate = LocalDate.parse(str, formatter);
                    Date dateValue = Date.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());

                    cell.setCellValue(com.fyntrac.common.utils.DateUtil.stripTime(dateValue));

                    CellStyle cellStyle = sheet.getWorkbook().createCellStyle();
                    DataFormat format = sheet.getWorkbook().createDataFormat();
                    cellStyle.setDataFormat(format.getFormat("MM/dd/yyyy"));
                    cell.setCellStyle(cellStyle);

                } else {
                    cell.setCellValue(strVal);
                }

            } else if (value instanceof BigDecimal bigDecimal) {
                double d = bigDecimal.doubleValue();
                if (bigDecimal.scale() <= 0 || d == Math.rint(d)) {
                    cell.setCellValue(bigDecimal.longValue());
                } else {
                    cell.setCellValue(d);
                }

                // Apply number format
                CellStyle style = sheet.getWorkbook().createCellStyle();
                DataFormat format = sheet.getWorkbook().createDataFormat();
                style.setDataFormat(format.getFormat("#,##0.##"));
                cell.setCellStyle(style);

            } else if (value instanceof Double d) {
                if (d == Math.rint(d)) {
                    cell.setCellValue(d.longValue());
                } else {
                    cell.setCellValue(d);
                }

                CellStyle style = sheet.getWorkbook().createCellStyle();
                DataFormat format = sheet.getWorkbook().createDataFormat();
                style.setDataFormat(format.getFormat("#,##0.##"));
                cell.setCellStyle(style);

            } else if (value instanceof Long l) {
                cell.setCellValue(l);

            } else if (value instanceof Date dateValue) {
                cell.setCellValue(com.fyntrac.common.utils.DateUtil.stripTime(dateValue));

                CellStyle cellStyle = sheet.getWorkbook().createCellStyle();
                DataFormat format = sheet.getWorkbook().createDataFormat();
                cellStyle.setDataFormat(format.getFormat("MM/dd/yyyy"));
                cell.setCellStyle(cellStyle);

            } else if (value == null) {
                cell.setCellValue(""); // or "N/A"

            } else {
                cell.setCellValue(value.toString());
            }
        }
    }

    public static boolean isValidDate(String dateStr) {
        // List of possible date formats (add more if needed)
        String[] possibleFormats = {
                "M/d/yyyy",   // e.g., 2/1/2022, 02/13/2022
                "M/d/yy",     // e.g., 2/1/22, 02/13/22
                "MM/dd/yyyy", // e.g., 02/01/2022
                "MM/dd/yy",   // e.g., 02/01/22
                "MM/dd/yyyy",  // e.g., 2/01/2022
                "M/dd/yy",    // e.g., 2/01/22
                "MM/d/yyyy",  // e.g., 02/1/2022
                "MM/d/yy"     // e.g., 02/1/22
        };

        for (String format : possibleFormats) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format, Locale.US);
                LocalDate.parse(dateStr, formatter);
                return true; // Successfully parsed
            } catch (DateTimeParseException e) {
                // Try next format
            }
        }
        return false; // All formats failed
    }

    // Helper: get cell value as trimmed string
    private static String getCellValue(Cell cell) {
        if (cell == null) return "";
        return cell.toString().trim();
    }


    // Utility: Case-insensitive fetch from Map
    private static Object getIgnoreCase(Map<String, Object> map, String key) {
        for (String k : map.keySet()) {
            if (k.equalsIgnoreCase(key)) {
                return map.get(k);
            }
        }
        return null;
    }

    private static Map<String, Integer> getColumnIndexMap(Row headerRow) {
        Map<String, Integer> columnIndexMap = new HashMap<>();
        for (Cell cell : headerRow) {
            columnIndexMap.put(cell.getStringCellValue().toUpperCase(), cell.getColumnIndex());
        }
        return columnIndexMap;
    }

    public static Workbook convertBinaryToWorkbook(Binary fileData) {
        if (fileData == null || fileData.getData() == null) {
            throw new IllegalArgumentException("The file data cannot be null");
        }

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(fileData.getData())) {
            // Use Apache POI to create a Workbook from the input stream
            return WorkbookFactory.create(inputStream);
        } catch (IOException e) {
            throw new RuntimeException("Error while converting Binary to Workbook", e);
        }
    }

    public static List<Map<String, Object>> readSheetAsListOfMaps(
            Workbook workbook, String sheetName, String requiredColumnName) {

        Sheet sheet = workbook.getSheet(sheetName);
        if (sheet == null) {
            throw new IllegalArgumentException("Sheet with name '" + sheetName + "' not found");
        }

        List<Map<String, Object>> result = new ArrayList<>();
        Iterator<Row> rowIterator = sheet.iterator();

        if (!rowIterator.hasNext()) {
            return result; // empty sheet
        }

        // First row as headers
        Row headerRow = rowIterator.next();
        List<String> headers = new ArrayList<>();
        for (Cell cell : headerRow) {
            headers.add(cell.getStringCellValue().trim());
        }

        int requiredColumnIndex = -1;
        String matchedRequiredColumnName = null;
        if (requiredColumnName != null && !requiredColumnName.isBlank()) {
            for (int i = 0; i < headers.size(); i++) {
                if (headers.get(i).equalsIgnoreCase(requiredColumnName.trim())) {
                    requiredColumnIndex = i;
                    matchedRequiredColumnName = headers.get(i); // keep original casing
                    break;
                }
            }
            if (requiredColumnIndex == -1) {
                throw new IllegalArgumentException(
                        "Required column '" + requiredColumnName + "' not found in headers");
            }
        }

        // Process rows
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Map<String, Object> rowMap = new LinkedHashMap<>();

            for (int i = 0; i < headers.size(); i++) {
                Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                Object value = getStringValue(cell);
                rowMap.put(headers.get(i), value);
            }

            // Only filter if requiredColumnName is set
            if (requiredColumnIndex != -1) {
                Object requiredValue = rowMap.get(matchedRequiredColumnName);
                if (requiredValue == null || requiredValue.toString().isBlank()) {
                    continue; // skip this row
                }
            }

            result.add(rowMap);
        }

        return result;
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
}
