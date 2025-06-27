package com.fyntrac.common.service;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.entity.ModelFile;
import com.fyntrac.common.exception.ExcelSheetNotFoundException;
import com.fyntrac.common.exception.HeaderNotFoundException;
import com.fyntrac.common.exception.MismatchException;
import com.fyntrac.common.utils.ExcelUtil;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.formula.eval.NotImplementedException;
import org.apache.poi.ss.usermodel.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.fyntrac.common.utils.ExcelUtil.getCellStringValue;

@Service
@Slf4j
public class ExcelFileService {

    public final static String TRANSACTION_SHEET_NAME="i_transaction";
    public final static String METRIC_SHEET_NAME="i_metric";
    public final static String EXECUTION_DATE_SHEET_NAME="i_executiondate";
    public final static String INSTRUMENT_ATTRIBUTE_SHEET_NAME= "i_instrumentattribute";
    public final static String OUTPUT_INSTRUMENT_ATTRIBUTE_SHEET_NAME= "o_instrumentattribute";
    public final static String OUTPUT_TRANSACTION_SHEET_NAME="o_transaction";
    protected static final List<String> ALLOWED_CONTENT_TYPES = List.of(
            "application/vnd.ms-excel", // .xls
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" // .xlsx
    );

    protected static final List<String> ALLOWED_EXTENSIONS = List.of("xls", "xlsx");

    protected final DataService<ModelFile> dataService;
    protected final DataService<Attributes> attributesDataService;


    @Autowired
    public ExcelFileService(DataService<ModelFile> dataService
    , DataService<Attributes> attributesDataService) {
        this.dataService = dataService;
        this.attributesDataService = attributesDataService;
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

        try{
        // Read all values from column A (index 0)
        for (int i = 1; i <= sheet.getLastRowNum(); i++) { // Start from 1 to skip header
            Row row = sheet.getRow(i);
            if (row != null) {
                Cell cell = row.getCell(0); // Column A (index 0)
                if (cell != null) {
                    switch (cell.getCellType()) {
                        case STRING:
                            values.add(cell.getStringCellValue());
                            break;
                        case FORMULA:
                            // Evaluate the formula to get the actual value
                            FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
                            CellValue cellValue = evaluator.evaluate(cell);
                            switch (cellValue.getCellType()) {
                                case STRING:
                                    values.add(cellValue.getStringValue());
                                    break;
                                case NUMERIC:
                                    values.add(String.valueOf(cellValue.getNumberValue()));
                                    break;
                                case BOOLEAN:
                                    values.add(String.valueOf(cellValue.getBooleanValue()));
                                    break;
                                case ERROR:
                                    values.add("ERROR");
                                    break;
                                default:
                                    values.add("UNKNOWN");
                                    break;
                            }
                            break;
                        default:
                            values.add(cell.toString());
                            break;
                    }
                    // values.add(cell.toString()); // Add cell value
                }
            }
        }
    }catch(NotImplementedException exp) {
        log.error(StringUtil.getStackTrace(exp));
        //persist error // to do
    }

        return values;
    }

    private static Set<String> getMetricDocumentFields() {
        String headers = "\"MetricName\",\"PostingDate\",\"Instrumentid\",\"AttributeId\",\"AccountingPeriod\",\"BeginningBalance\",\"Activity\",\"EndingBalance\"";

        // Convert to Set<String>
        Set<String> headerSet = Arrays.stream(headers.split(","))
                .map(header -> header.replace("\"", "").trim()) // Remove quotes and trim
                .collect(Collectors.toSet());

        return headerSet;
    }

    private static Set<String> getTransactionActivityFields() {
        String headers = "\"TransactionName\",\"PostingDate\",\"Instrumentid\",\"AttributeId\",\"TransactionDate\",\"Amount\"";

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

        excelHeaderSet.removeIf(String::isEmpty);

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
        if(collectionName.equalsIgnoreCase("TransactionActivity")){
            mongoFields.addAll(ExcelFileService.getTransactionActivityFields());
        }
        return areHeadersSubsetOfDocumentFields(mongoFields, excelHeaders, sheetName);
    }

    public boolean validateInstrumentAttributeColumns(Workbook workbook, String sheetName) throws Exception {

        Collection<Attributes> fields = this.attributesDataService.fetchAllData(Attributes.class);
        Set<String> mongoFields = new HashSet<>(0);
        List<String> excelHeaders = getExcelHeaders(workbook, sheetName);
        for(Attributes attribute : fields) {
            mongoFields.add(String.format("%s.%s","ATTRIBUTES",attribute.getAttributeName()));
        }
        mongoFields.add("Type");
        mongoFields.add("PostingDate");
        mongoFields.add("EffectiveDate");
        mongoFields.add("InstrumentId");
        mongoFields.add("AttributeID");

        return areHeadersSubsetOfDocumentFields(mongoFields, excelHeaders, sheetName);
    }

    public boolean validateTransactionActivityColumns(Workbook workbook, String sheetName) throws Exception {
        return compareExcelWithMongo(workbook, sheetName, "TransactionActivity");
    }

    public boolean validateMetricColumns(Workbook workbook) throws Exception {
        List<String> excelHeaders = getExcelHeaders(workbook, METRIC_SHEET_NAME);
        Set<String> mongoFields = getMetricDocumentFields();
        return areHeadersSubsetOfDocumentFields(mongoFields, excelHeaders, METRIC_SHEET_NAME);
    }

    /**
     * Reads the first sheet of the provided Workbook and extracts all rows' MetricName and AttributeId by given headers.
     *
     * @param workbook           The Apache POI Workbook instance (XLSX or XLS)
     * @param metricNameHeader   The header name for the MetricName column
     * @param attributeIdHeader  The header name for the AttributeId column
     * @return List of MetricAttributeRow ordered by row number; empty list if headers not found or no data rows
     * @throws IOException If workbook processing fails (rare for this method)
     */
    public List<Records.MetricAttributeRow> readSheetByHeaders(Workbook workbook,String sheetName, String metricNameHeader, String attributeIdHeader) throws IOException {
        List<Records.MetricAttributeRow> resultList = new ArrayList<>();

        Sheet sheet = this.getSheet(workbook, sheetName); // First sheet

        if (sheet == null) {
            return resultList; // Empty list if no sheet
        }

        Row headerRow = sheet.getRow(0);
        if (headerRow == null) {
            return resultList; // Empty if no header row
        }

        int metricNameCol = -1;
        int attributeIdCol = -1;

        // Identify column indices for the headers (case-insensitive)
        for (Cell cell : headerRow) {
            if (cell.getCellType() == CellType.STRING) {
                String header = cell.getStringCellValue().trim();
                if (header.equalsIgnoreCase(metricNameHeader)) {
                    metricNameCol = cell.getColumnIndex();
                } else if (header.equalsIgnoreCase(attributeIdHeader)) {
                    attributeIdCol = cell.getColumnIndex();
                }
            }
        }

        if (metricNameCol == -1 || attributeIdCol == -1) {
            // Headers not found, return empty list
            return resultList;
        }

        int lastRow = sheet.getLastRowNum();
        for (int rowIndex = 1; rowIndex <= lastRow; rowIndex++) {
            Row row = sheet.getRow(rowIndex);
            if (row == null) {
                continue; // skip empty row
            }

            String metricNameValue = ExcelUtil.getCellStringValue(row.getCell(metricNameCol));
            String attributeIdValue = ExcelUtil.getCellStringValue(row.getCell(attributeIdCol));

            resultList.add(RecordFactory.createMetricAttributeRow(rowIndex + 1, metricNameValue, attributeIdValue));
        }

        return resultList;
    }


}
