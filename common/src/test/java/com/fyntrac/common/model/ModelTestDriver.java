package com.fyntrac.common.model;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class ModelTestDriver {
    @Autowired
    private TestRestTemplate restTemplate;
    private String propertyFilePath;
    private String excelTestFilePath;
    private static final String DEFAULT_PROPERTY_FILE = "TestDriver/TestSBO/test.properties"; // Default path
    private static final String DEFAULT_EXCEL_FILE = "TestDriver/TestSBO/api_calls.xlsx"; // Default path
    private String tenantId; // Variable to hold tenantId

    @BeforeEach
    public void setUp() {
        // Use system properties for file paths or fallback to defaults
        propertyFilePath = System.getProperty("propertyFilePath", DEFAULT_PROPERTY_FILE);
        excelTestFilePath = System.getProperty("excelTestFilePath", DEFAULT_EXCEL_FILE);
        // Set tenantId from system property or default value
        tenantId = System.getProperty("tenantId", "defaultTenantId"); // Replace with actual default if needed
        System.out.println("Using database file: " + propertyFilePath);
        System.out.println("Using excel file: " + excelTestFilePath);
        System.out.println("Using tenant ID: " + tenantId);
    }
}
