package com.reserv.dataloader.service;

import com.reserv.dataloader.exception.ExcelSheetNotFoundException;
import com.reserv.dataloader.exception.HeaderNotFoundException;
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
import java.util.ArrayList;
import java.util.List;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
@Slf4j
public class ExcelFileService {

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

    public List<String> readExcelSheet(Workbook workbook, String sheetName) throws Exception{
        List<String> values = new ArrayList<>(0);
        try{


            Sheet sheet = this.getSheet(workbook, sheetName);

            if (sheet == null) {
                throw new ExcelSheetNotFoundException("Sheet '" + sheetName + "' not found.");
            }

            // Read the header (first row)
            Row headerRow = sheet.getRow(0);
            if (headerRow != null) {
                StringBuilder header = new StringBuilder();
                for (Cell cell : headerRow) {
                    header.append(cell.toString() + "\t"); // Print header values
                }
                log.info(header.toString());
            }else{
                throw new HeaderNotFoundException("Sheet [" + sheetName + "] header not found, invalid sheet");
            }

            // Read all values from column A (index 0)
            for (int i = 1; i <= sheet.getLastRowNum(); i++) { // Start from 1 to skip header
                Row row = sheet.getRow(i);
                if (row != null) {
                    Cell cell = row.getCell(0); // Column A (index 0)
                    if (cell != null) {
                        values.add(cell.toString());// add cell value
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return values;
    }
}
