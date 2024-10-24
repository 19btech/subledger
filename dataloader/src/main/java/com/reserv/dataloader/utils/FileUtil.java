package com.reserv.subledger.utils;

import com.fyntrac.common.enums.AccountingRules;
import com.reserv.dataloader.exception.ExcelFormulaCellException;
import org.springframework.web.multipart.MultipartFile;
import org.apache.poi.ss.usermodel.*;
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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


public class FileUtil {

    public File saveFileToTempFolder(MultipartFile file, String destinationDirectory) throws Exception{

        org.apache.commons.io.FileUtils.deleteQuietly(new File(destinationDirectory + File.separator +file.getOriginalFilename()));

        File uploadedFile = new File(destinationDirectory + File.separator +file.getOriginalFilename());

        uploadedFile.createNewFile();
        FileOutputStream fos =new FileOutputStream(uploadedFile);
        fos.write(file.getBytes());
        fos.close();

        return uploadedFile;
    }

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
                                writer.append(((DateUtil.isCellDateFormatted(cell)) ? getDateValue(cell) : getNumericValue(cell)) + "");
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

    private static String getDateValue(Cell cell) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date date = cell.getDateCellValue();
        return  df.format(date);
    }

    private static String getNumericValue(Cell cell) {
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
    private static void setFormulaCellValue(Cell cell,BufferedWriter data,FormulaEvaluator evaluator,
                                            int cellIndex) throws Exception {

        try {
            CellValue cellValue = evaluator.evaluate(cell);
            switch(cellValue.getCellType()) {
                case BOOLEAN:
                    data.append(cellValue.getBooleanValue() + "");
                    break;
                case NUMERIC:
                    data.append(cellValue.getNumberValue() + "");
                    break;
                default:
                    data.append("\"" + cellValue.getStringValue() + "\"" );
            }
        }catch (Exception exp) {
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


    public static String convertMultipartFileToFile(MultipartFile multipartFile, String filePath) throws IOException {
        // Create a temporary file
        String fileName =  getFileNameWithoutExtension(multipartFile);
        String fileExtention = getFileExtension(multipartFile);
        Path path = Path.of(filePath + File.separator + fileName + "." + fileExtention);
        // Copy the contents of the multipart file to the temporary file
        try {
            createDirectory(filePath);
            Files.deleteIfExists(path);
            File createdFile = path.toFile();
            if (createdFile.exists()) {
                createdFile.delete();
            }
            multipartFile.transferTo(createdFile);
        } catch (IOException e) {
            throw e;
        }

        return path.toAbsolutePath().toString();
    }

    public static String getFileExtension(MultipartFile multipartFile) {
        // Get the original filename
        String originalFilename = multipartFile.getOriginalFilename();

        // Check if the filename is not null and has an extension
        if (originalFilename != null && originalFilename.contains(".")) {
            // Extract and return the file extension
            return originalFilename.substring(originalFilename.lastIndexOf(".") + 1).toLowerCase();
        }

        // If filename does not contain an extension or is null, return an empty string
        return "";
    }

    public static String getFileNameWithoutExtension(MultipartFile multipartFile) {
        // Get the original filename
        String originalFilename = multipartFile.getOriginalFilename();

        // Check if the filename is not null and has an extension
        if (originalFilename != null && originalFilename.contains(".")) {
            // Extract and return the filename without the extension
            return originalFilename.substring(0, originalFilename.lastIndexOf("."));
        }

        // If filename does not contain an extension or is null, return the original filename
        return originalFilename;
    }

    public static Set<File> unzip(MultipartFile multipartFile, String destDirectory) throws IOException {
        File destDir = new File(destDirectory);
        Set<File> fileSet = new HashSet<>(0);
        if (destDir.exists()) {
            destDir.deleteOnExit();
        }else {
            destDir.mkdirs();
        }

        try (ZipInputStream zipIn = new ZipInputStream(multipartFile.getInputStream())) {
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                String filePath = destDirectory + File.separator + entry.getName();
                if (!entry.isDirectory()) {
                    File file = new File(filePath);
                    extractFile(zipIn, file);
                    fileSet.add(file);
                } else {
                    File dir = new File(filePath);
                    dir.mkdirs();
                }
                zipIn.closeEntry();

            }
        }
        return fileSet;
    }

    private static void extractFile(ZipInputStream zipIn, File file) throws IOException {
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

    public static boolean isZipFile(MultipartFile file) {
        // Get the original file name
        String originalFilename = file.getOriginalFilename();

        // Check if the original file name ends with ".zip" (case-insensitive)
        return originalFilename != null && originalFilename.toLowerCase().endsWith(".zip");
    }

    public static boolean isExtensionMatched(String fileName, String expectedExtension) {
        // Get the file extension from the file name
        String fileExtension = getFileExtension(fileName);

        // Check if the file extension matches the expected extension
        return fileExtension != null && fileExtension.equalsIgnoreCase(expectedExtension);
    }

    private static String getFileExtension(String fileName) {
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
}

