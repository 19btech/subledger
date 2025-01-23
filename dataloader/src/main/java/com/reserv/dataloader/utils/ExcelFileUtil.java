package com.reserv.dataloader.utils;

import  com.fyntrac.common.enums.AccountingRules;
import com.fyntrac.common.exception.ExcelFormulaCellException;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
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
import com.fyntrac.common.utils.ExcelUtil;

public class ExcelFileUtil extends ExcelUtil{

    public File saveFileToTempFolder(MultipartFile file, String destinationDirectory) throws Exception{

        org.apache.commons.io.FileUtils.deleteQuietly(new File(destinationDirectory + File.separator +file.getOriginalFilename()));

        File uploadedFile = new File(destinationDirectory + File.separator +file.getOriginalFilename());

        uploadedFile.createNewFile();
        FileOutputStream fos =new FileOutputStream(uploadedFile);
        fos.write(file.getBytes());
        fos.close();

        return uploadedFile;
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


    public static boolean isZipFile(MultipartFile file) {
        // Get the original file name
        String originalFilename = file.getOriginalFilename();

        // Check if the original file name ends with ".zip" (case-insensitive)
        return originalFilename != null && originalFilename.toLowerCase().endsWith(".zip");
    }

    public static byte[] convertIntByteArray(MultipartFile multipartFile) throws IOException {
        // Create a workbook from the multipart file
        Workbook workbook;
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(multipartFile.getBytes())) {
            // Assuming the input file is in XLSX format
            workbook = new XSSFWorkbook(inputStream);
        }

        // Write the workbook to a byte array output stream
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            workbook.write(outputStream);
            return outputStream.toByteArray(); // Return the byte array
        } finally {
            workbook.close(); // Ensure the workbook is closed
        }
    }

    // Method to convert MultipartFile to Workbook
    public static Workbook convertMultipartFileToWorkbook(MultipartFile multipartFile) throws IOException {
        String fileName = multipartFile.getOriginalFilename();
        if (fileName == null) {
            throw new IllegalArgumentException("File name cannot be null");
        }

        // Check the file extension
        String fileExtension = getFileExtension(fileName);
        Workbook workbook;

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(multipartFile.getBytes())) {
            assert fileExtension != null;
            if (fileExtension.equalsIgnoreCase("xls")) {
                // Handle XLS format
                workbook = new HSSFWorkbook(inputStream);
            } else if (fileExtension.equalsIgnoreCase("xlsx")) {
                // Handle XLSX format
                workbook = new XSSFWorkbook(inputStream);
            } else {
                throw new IllegalArgumentException("Unsupported file format: " + fileExtension);
            }
        }

        return workbook; // Return the Workbook object
    }


    // Validate if the file is a valid Excel file
    public static boolean isValidExcelFile(MultipartFile file) {
        String contentType = file.getContentType();
        String originalFilename = file.getOriginalFilename();

        return (contentType != null &&
                (contentType.equals("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") ||
                        contentType.equals("application/vnd.ms-excel"))) &&
                (originalFilename != null &&
                        (originalFilename.endsWith(".xls") || originalFilename.endsWith(".xlsx")));
    }

}

