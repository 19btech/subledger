package com.reserv.dataloader.service;

import com.reserv.dataloader.exception.ExcelSheetNotFoundException;
import com.reserv.dataloader.exception.HeaderNotFoundException;
import com.reserv.dataloader.exception.MismatchException;
import com.reserv.dataloader.utils.ExcelFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.bson.types.Binary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import com.fyntrac.common.entity.ModelFile;
import com.fyntrac.common.service.DataService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
@Slf4j
public class ExcelFileService {

    public final static String TRANSACTION_SHEET_NAME="i_transaction";
    public final static String METRIC_SHEET_NAME="i_metric";
    public final static String EXECUTION_DATE_SHEET_NAME="i_executiondate";
    public final static String INSTRUMENT_ATTRIBUTE_SHEET_NAME= "i_instrumentattribute";

    private static final List<String> ALLOWED_CONTENT_TYPES = List.of(
            "application/vnd.ms-excel", // .xls
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" // .xlsx
    );

    private static final List<String> ALLOWED_EXTENSIONS = List.of("xls", "xlsx");

    private final DataService<ModelFile> dataService;

    @Autowired
    public ExcelFileService(DataService<ModelFile> dataService) {
        this.dataService = dataService;
    }

    // Upload file to MongoDB
    public String uploadFile(MultipartFile file) throws IOException {
        validateFile(file);

        ModelFile fileDocument = new ModelFile();
        fileDocument.setContentType(file.getContentType());
        Workbook workbook = ExcelFileUtil.convertMultipartFileToWorkbook(file);
        byte[] bytes = ExcelFileUtil.convertWorkbookToByteArray(workbook);
        fileDocument.setFileData(new Binary(bytes));

        ModelFile savedDocument = this.dataService.save(fileDocument);
        return savedDocument.getId();
    }

    public boolean validateModel(Workbook workbook) {

        return false;
    }

    // Retrieve file and convert it into Excel format
    public byte[] getExcelFile(String fileId) {

        Query query = new Query();
        query.addCriteria(Criteria.where("id").is(fileId));
        List<ModelFile> fileDocuments = this.dataService.fetchData(query, ModelFile.class);

        if (fileDocuments == null || fileDocuments.isEmpty()) {
            throw new ResponseStatusException(NOT_FOUND, "File not found with ID: " + fileId);
        }

        ModelFile fileDocument = fileDocuments.get(0);
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(fileDocument.getFileData().getData());
             Workbook workbook = WorkbookFactory.create(inputStream);
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            workbook.write(outputStream);
            return outputStream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException("Failed to convert file to Excel format", e);
        }
    }

    // Validate file type
    private void validateFile(MultipartFile file) {
        String contentType = file.getContentType();
        String extension = FilenameUtils.getExtension(file.getOriginalFilename());

        if (!ALLOWED_CONTENT_TYPES.contains(contentType) || !ALLOWED_EXTENSIONS.contains(extension)) {
            throw new IllegalArgumentException("Invalid file type. Only .xls and .xlsx files are allowed.");
        }
    }

    public Sheet getSheet(Workbook workbook, String sheetName) {
        // Get the sheet by name
        // Iterate through all sheets to find a match
        for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
            Sheet sheet = workbook.getSheetAt(i);
            if (sheet.getSheetName().equalsIgnoreCase(sheetName)) {
                return sheet; // Return the matching sheet
            }
        }
        return null;
    }

    public List<String> readExcelSheet(Workbook workbook, String sheetName) throws ExcelSheetNotFoundException, HeaderNotFoundException {
        List<String> values = new ArrayList<>();

        Sheet sheet = this.getSheet(workbook, sheetName);

        if (sheet == null) {
            throw new ExcelSheetNotFoundException("Sheet '" + sheetName + "' not found.");
        }

        // Read the header (first row)
        Row headerRow = sheet.getRow(0);
        if (headerRow == null) {
            throw new HeaderNotFoundException("Sheet [" + sheetName + "] header not found, invalid sheet.");
        }

        // Logging header values
        StringBuilder header = new StringBuilder();
        for (Cell cell : headerRow) {
            header.append(cell.toString()).append("\t");
        }
        log.info("Header: {}", header.toString());

        // Read all values from column A (index 0)
        for (int i = 1; i <= sheet.getLastRowNum(); i++) { // Start from 1 to skip header
            Row row = sheet.getRow(i);
            if (row != null) {
                Cell cell = row.getCell(0); // Column A (index 0)
                if (cell != null) {
                    values.add(cell.toString()); // Add cell value
                }
            }
        }

        return values;
    }

    private static Set<String> getMetricDocumentFields() {
        String headers = "\"MetricName\",\"Instrumentid\",\"AttributeId\",\"AccountingPeriod\",\"Balance.BeginningBalance\",\"Balance.Activity\",\"Balance.EndingBalance\"";

        // Convert to Set<String>
        Set<String> headerSet = Arrays.stream(headers.split(","))
                .map(header -> header.replace("\"", "").trim()) // Remove quotes and trim
                .collect(Collectors.toSet());

        return headerSet;
    }


    public Set<String> getColumnNamesFromDocument(String documentName) {
        Set<String> columns = new HashSet<>();

        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate();
        // Get one document to extract the field names (columns)
        Map<String, Object> doc = mongoTemplate.findOne(Query.query(Criteria.where("_id").exists(true)), Map.class, documentName);

        if (doc != null) {
            extractKeys(doc, columns, "");
        }

        return columns;
    }

    private void extractKeys(Map<String, Object> map, Set<String> columns, String parentKey) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = parentKey.isEmpty() ? entry.getKey() : parentKey + "." + entry.getKey();

            // Add the current key
            columns.add(key);

            // If the value is a nested object (map), recurse
            if (entry.getValue() instanceof Map) {
                extractKeys((Map<String, Object>) entry.getValue(), columns, key);
            }
        }
    }

    // Get Excel headers
    private List<String> getExcelHeaders(Workbook workbook, String sheetName) throws Exception {
        Sheet sheet = this.getSheet(workbook, sheetName);

        if (sheet == null) {
            throw new ExcelSheetNotFoundException("Sheet '" + sheetName + "' not found.");
        }

        List<String> headers = new ArrayList<>();
        try {
            Row headerRow = sheet.getRow(0);
            if (headerRow != null) {
                for (Cell cell : headerRow) {
                    headers.add(cell.getStringCellValue().trim()); // Trim whitespace
                }
            }
        } catch (NullPointerException e) {
            // Handle case where the header row or cell is null
            throw new Exception("Error reading headers: " + e.getMessage(), e);
        } catch (IllegalStateException e) {
            // Handle case where the cell type is not a string
            throw new Exception("Error reading headers: Cell type is not a string.", e);
        } catch (Exception e) {
            // Catch any other exceptions
            throw new Exception("An unexpected error occurred while reading headers: " + e.getMessage(), e);
        }

        return headers;
    }

    // Check if all headers exist in document fields
    private boolean areHeadersSubsetOfDocumentFields(Set<String> mongoFields, List<String> excelHeaders, String sheetName) {
        Set<String> excelHeaderSet = excelHeaders.stream()
                .map(String::toLowerCase) // Normalize to lowercase
                .collect(Collectors.toSet());

        Set<String> mongoFieldSet = mongoFields.stream()
                .map(String::toLowerCase) // Normalize to lowercase
                .collect(Collectors.toSet());

        // Find missing headers
        Set<String> missingHeaders = new HashSet<>(excelHeaderSet);
        missingHeaders.removeAll(mongoFieldSet);

        // If there are missing headers, throw MismatchException
        if (!missingHeaders.isEmpty()) {
            throw new MismatchException("Sheet[" + sheetName + "] has following headers are missing in MongoDB fields: " + missingHeaders);
        }
        return Boolean.TRUE;
    }

    // Method to compare headers with MongoDB document fields
    public boolean compareExcelWithMongo(Workbook workbook, String sheetName, String collectionName) throws Exception {
        List<String> excelHeaders = getExcelHeaders(workbook, sheetName);
        Set<String> mongoFields = getColumnNamesFromDocument(collectionName);
        return areHeadersSubsetOfDocumentFields(mongoFields, excelHeaders, sheetName);
    }

    public boolean validateInstrumentAttributeColumns(Workbook workbook) throws Exception {

        List<String> excelHeaders = getExcelHeaders(workbook, INSTRUMENT_ATTRIBUTE_SHEET_NAME);
        Set<String> mongoFields = getColumnNamesFromDocument("InstrumentAttribute");
        mongoFields.add("Type");
        return areHeadersSubsetOfDocumentFields(mongoFields, excelHeaders, INSTRUMENT_ATTRIBUTE_SHEET_NAME);
    }

    public boolean validateTransactionActivityColumns(Workbook workbook) throws Exception {
        return compareExcelWithMongo(workbook, TRANSACTION_SHEET_NAME, "TransactionActivity");
    }

    public boolean validateMetricColumns(Workbook workbook) throws Exception {
        List<String> excelHeaders = getExcelHeaders(workbook, METRIC_SHEET_NAME);
        Set<String> mongoFields = getMetricDocumentFields();
        return areHeadersSubsetOfDocumentFields(mongoFields, excelHeaders, METRIC_SHEET_NAME);
    }
}
