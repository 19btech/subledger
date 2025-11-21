package com.fyntrac.common.service;

import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Service
@Slf4j
public class GenericJsonImportService {

    private DataService dataService;

    public GenericJsonImportService(DataService dataService) {
        this.dataService = dataService;
    }

    public void setDataService(DataService dataService) {
        this.dataService = dataService;
    }
    /**
     * Push JSON array directly to any collection
     */
    public ImportResult importJsonToCollection(InputStream jsonInputStream, String collectionName, String tenantId) {
        try {
            // Read JSON content from InputStream
            String jsonContent = StreamUtils.copyToString(jsonInputStream, StandardCharsets.UTF_8);

            // Parse JSON to MongoDB Documents
            List<Document> documents = parseJsonToDocuments(jsonContent);

            // Insert into specified collection
            return saveDocuments(documents, collectionName, tenantId);

        } catch (IOException e) {
            log.error("Failed to read JSON input stream", e);
            return ImportResult.failure("Failed to read JSON: " + e.getMessage());
        } catch (Exception e) {
            log.error("Failed to parse JSON", e);
            return ImportResult.failure("Failed to parse JSON: " + e.getMessage());
        }
    }

    /**
     * Parse JSON string to MongoDB Documents
     */
    private List<Document> parseJsonToDocuments(String jsonContent) {
        // Remove whitespace and check if it's an array
        String trimmedContent = jsonContent.trim();

        if (trimmedContent.startsWith("[")) {
            // JSON array - parse as array of documents
            return Document.parse("{ \"docs\": " + trimmedContent + " }")
                    .getList("docs", Document.class);
        } else {
            // Single JSON object - wrap in list
            Document singleDoc = Document.parse(trimmedContent);
            return List.of(singleDoc);
        }
    }

    /**
     * Save documents to specified collection
     */
    private ImportResult saveDocuments(List<Document> documents, String collectionName, String tenantId) {
        if (documents == null || documents.isEmpty()) {
            return ImportResult.failure("No documents found in JSON");
        }

        try {
            // Drop existing collection if you want fresh data (optional)
            // mongoTemplate.dropCollection(collectionName);

            // Insert all documents
            this.dataService.setTenantId(tenantId);
            this.dataService.getMongoTemplate().insert(documents, collectionName);

            log.info("Successfully imported {} documents into collection '{}'",
                    documents.size(), collectionName);

            return ImportResult.success(documents.size(), collectionName);

        } catch (Exception e) {
            log.error("Failed to save documents to collection '{}'", collectionName, e);
            return ImportResult.failure("Failed to save to database: " + e.getMessage());
        }
    }

    // Import Result class
    public static class ImportResult {
        private final boolean success;
        private final String message;
        private final int importedCount;
        private final String collectionName;

        private ImportResult(boolean success, String message, int importedCount, String collectionName) {
            this.success = success;
            this.message = message;
            this.importedCount = importedCount;
            this.collectionName = collectionName;
        }

        public static ImportResult success(int count, String collectionName) {
            return new ImportResult(true,
                    "Successfully imported " + count + " documents into '" + collectionName + "'",
                    count, collectionName);
        }

        public static ImportResult failure(String message) {
            return new ImportResult(false, message, 0, null);
        }

        // Getters
        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public int getImportedCount() { return importedCount; }
        public String getCollectionName() { return collectionName; }
    }
}