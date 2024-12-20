package com.reserv.dataloader.service;

import org.apache.commons.io.FilenameUtils;
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
import java.util.List;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
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
        fileDocument.setFileData(new Binary(file.getBytes()));

        ModelFile savedDocument = this.dataService.save(fileDocument);
        return savedDocument.getId();
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
}
