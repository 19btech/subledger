package com.fyntrac.common.utils;

import  com.fyntrac.common.enums.AccountingRules;
import com.fyntrac.common.exception.ExcelFormulaCellException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.*;
import org.bson.types.Binary;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipInputStream;


@Slf4j
public class ExcelUtil {


    public  static File saveFileToTempFolder(String filePath, String fileName, byte[] fildData) throws Exception{

        org.apache.commons.io.FileUtils.deleteDirectory(new File(filePath));
        new File(filePath + File.separator).mkdirs();
        File uploadedFile = new File(filePath + File.separator + fileName);

        uploadedFile.createNewFile();
        FileOutputStream fos =new FileOutputStream(uploadedFile);
        fos.write(fildData);
        fos.close();

        return uploadedFile;
    }

    public File convertXlsToCSV(File file) {

        return null;
    }


    public  String getHeaderOfCSVFile(File csvFile){
        BufferedReader br = null;
        String strLine = "";
        try {
            LineNumberReader reader = new LineNumberReader(new InputStreamReader(new FileInputStream(csvFile.getAbsoluteFile()), "UTF-8"));
            if(((strLine = reader.readLine()) != null) && reader.getLineNumber() == 1)
            {
                return strLine;
            }

            reader.close();

        } catch (FileNotFoundException e) {
            System.err.println("File not found");
        } catch (IOException e) {
            System.err.println("Unable to read the file.");
        }
        return null;
    }

    public static List<String> getSheetListFromExcelFile(String fileNameWithAbsolutePath) throws Throwable{

        List<String> xlSheetList = new ArrayList<>(0);
        try {
            InputStream inp = new FileInputStream(fileNameWithAbsolutePath);
            Workbook wb = WorkbookFactory.create(inp);
            for (Sheet sheet : wb) {
                xlSheetList.add(sheet.getSheetName());
            }
        }catch (IOException ex) {
            throw ex;
        }

        return xlSheetList;
    }

    public static Set<String> getSheetNameList(String fileNameWithAbsolutePath) throws Throwable{
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
    public  static void convertExcelToCSV(String fileNameWithAbsolutePath,
                                          String outPutFilePath,
                                          Long activityUploadId) throws Throwable{
        InputStream inp = null;
        try {
            inp = new FileInputStream(fileNameWithAbsolutePath);
            Workbook wb = WorkbookFactory.create(inp);
            FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();
            for (Sheet sheet : wb) {

                if(!AccountingRules.isValid(sheet.getSheetName().toLowerCase() + ".csv")) {
//                    throw new InvalidExcelSheetNameException("Invalid SheetName["
//                            + sheet.getSheetName() + "] of file[" + "]");
                    continue;
                }

                String sheetName = sheet.getSheetName().toLowerCase();
                String outputFile = outPutFilePath + File.separator +  sheetName + ".csv";
                Path outputFilePath = Path.of(outputFile);

                if(!Files.exists(outputFilePath.getParent())) {
                    Files.createDirectories(outputFilePath.getParent());
                }
//                Files.deleteIfExists(outputFilePath);
//                Files.createFile(outputFilePath);
                BufferedWriter writer = new BufferedWriter(new FileWriter(outPutFilePath + File.separator +  sheetName + ".csv", true));
                boolean isFirstRow = Boolean.TRUE;
                int numOfCells = 0;

                Iterator<Row> rowIterator = sheet.rowIterator();
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    if(isRowEmpty(row)) {
                        continue;
                    }
                    boolean isFirstCell = Boolean.TRUE;
                    if(isFirstRow) {
                        numOfCells = row.getPhysicalNumberOfCells();
                    }

                    for (int cellIndex = 0; cellIndex < numOfCells; cellIndex++) {
                        Cell cell = row.getCell(cellIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                        if(isFirstRow && activityUploadId != null) {
                            isFirstRow = Boolean.FALSE;
                            isFirstCell = Boolean.FALSE;
                            writer.append("\"activityUploadId\"".toUpperCase());
                            writer.append(',');
                        }else if(isFirstCell){
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
                                        + "\"" );
                                break;
                            case BLANK:
                                writer.append("\"" + "\"" );
                                break;
                            case FORMULA:
                                setFormulaCellValue(cell, writer, evaluator, cellIndex);
                                break;
                            case _NONE:
                                writer.append("\"" + "\"" );
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


    /**
     * Removes columns with empty headers from a CSV file and overwrites the original file.
     *
     * @param inputPath Path to the input CSV file.
     * @throws IOException If an I/O error occurs.
     */
    public static void removeEmptyHeaderColumns(Path inputPath) throws IOException {
        Path tempFile = Files.createTempFile("cleaned_", ".csv");

        try (
                BufferedReader reader = Files.newBufferedReader(inputPath);
                BufferedWriter writer = Files.newBufferedWriter(tempFile)
        ) {
            // Read header line
            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new IllegalArgumentException("File is empty: " + inputPath);
            }

            String[] headers = headerLine.split(",");
            List<Integer> validIndices = new ArrayList<>();
            List<String> validHeaders = new ArrayList<>();

            for (int i = 0; i < headers.length; i++) {
                String header = headers[i].trim();
                if (!header.isEmpty()) {
                    validIndices.add(i);
                    validHeaders.add(header);
                }
            }

            // Write cleaned header
            writer.write(String.join(",", validHeaders));
            writer.newLine();

            // Now read and clean data rows
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",", -1); // -1 to keep trailing empty strings
                List<String> validValues = new ArrayList<>();

                for (int i : validIndices) {
                    validValues.add(i < values.length ? values[i] : "");
                }

                writer.write(String.join(",", validValues));
                writer.newLine();
            }
        }

        // Replace original file
        Files.move(tempFile, inputPath, StandardCopyOption.REPLACE_EXISTING);
    }


    protected static String getDateValue(Cell cell) {
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
        Date date = cell.getDateCellValue();
        return  df.format(date);
    }

    protected static String getNumericValue(Cell cell) {
        Double value = 0.0d;
        if (cell.getCellStyle().getDataFormatString().contains("%")) {
            // Detect Percent Values
            value = cell.getNumericCellValue() / 100;
            System.out.println("Percent value found = " + value.toString() +"%");
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
                    if (DateUtil.isCellDateFormatted(cell)) {
                        Date date = cell.getDateCellValue();
                        // Format the date as needed, e.g., "yyyy-MM-dd"
                        SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
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
        if(row != null) {
            for(Cell cell: row) {
                if(dataFormatter.formatCellValue(cell).trim().length() > 0) {
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
                System.err.println("Failed to create directory: " + directoryPath);
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
    public static void mapJsonToExcel(List<Map<String, Object>> jsonData, Workbook workbook, String sheetName) throws IOException {
        // Step 2: Load Excel File
        Sheet sheet = getSheetIgnoreCase(workbook, sheetName); // Update with your actual sheet name
        log.info("Sheet Name:", sheetName);
        log.info("Excel Sheet Name", sheet.getSheetName());
        // Step 3: Get Excel Headers (Row 0)
        Row headerRow = sheet.getRow(0);
        Map<String, Integer> excelColumnMap = getColumnIndexMap(headerRow);
        cleanSheet(sheet);
        // Step 4: Write JSON Data to Excel Based on Matching Headers
        int rowNum = sheet.getLastRowNum() + 1; // Append new rows
        for (Map<String, Object> row : jsonData) {
            Row excelRow = sheet.createRow(rowNum++);
            for (String column : row.keySet()) {
                if (excelColumnMap.containsKey(column)) {
                    int colIndex = excelColumnMap.get(column);
                    excelRow.createCell(colIndex).setCellValue(row.get(column).toString());
                }
            }
        }
    }

    public static Map<String, Integer> getColumnIndexMap(Row headerRow) {
        Map<String, Integer> columnIndexMap = new HashMap<>();
        for (Cell cell : headerRow) {
            columnIndexMap.put(cell.getStringCellValue(), cell.getColumnIndex());
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

    public static String getCellStringValue(Cell cell) {
        if (cell == null) {
            return "";
        }

        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue().trim();
            case NUMERIC -> {
                double d = cell.getNumericCellValue();
                if (d == (long) d) {
                    yield String.valueOf((long) d);
                } else {
                    yield String.valueOf(d);
                }
            }
            case BOOLEAN -> String.valueOf(cell.getBooleanCellValue());
            case FORMULA -> {
                FormulaEvaluator evaluator = cell.getSheet().getWorkbook().getCreationHelper().createFormulaEvaluator();
                CellValue cellValue = evaluator.evaluate(cell);
                yield switch (cellValue.getCellType()) {
                    case STRING -> cellValue.getStringValue().trim();
                    case NUMERIC -> {
                        double d = cellValue.getNumberValue();
                        if (d == (long) d) {
                            yield String.valueOf((long) d);
                        } else {
                            yield String.valueOf(d);
                        }
                    }
                    case BOOLEAN -> String.valueOf(cellValue.getBooleanValue());
                    default -> "";
                };
            }
            case BLANK, ERROR, _NONE -> "";
        };
    }
}

