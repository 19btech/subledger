package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.enums.DataType;
import com.fyntrac.common.repository.AttributesRepository;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import com.reserv.dataloader.validation.AttributesValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AttributesItemProcessorTest {

    @Mock
    private AttributesRepository attributesRepository;

    @Mock
    private RefDataValidationLogRepository validationLogRepository;

    private AttributesValidator validator;
    private AttributesItemProcessor processor;

    @BeforeEach
    void setUp() {
        validator = new AttributesValidator();
        processor = new AttributesItemProcessor(validator, attributesRepository, validationLogRepository);

        JobExecution jobExecution = new JobExecution(101L);
        StepExecution stepExecution = new StepExecution("attributeImportStep", jobExecution);
        
        // Skip active tenant loading for most tests unless explicitly configured in stepExecution parameters
        processor.beforeStep(stepExecution);
    }

    @Test
    void testValidAttributeRecordPassesSuccessfully() throws Exception {
        Attributes item = new Attributes();
        item.setAttributeName("valid_attribute_name");
        item.setUserField("Field A");
        item.setIsReclassable(1); // True
        item.setIsVersionable(1); // True
        item.setRawDataType("String");
        item.setRawNullable("Yes");

        Attributes result = processor.process(item);

        assertNotNull(result);
        assertEquals("valid_attribute_name", result.getAttributeName());
        assertEquals(1, result.getIsReclassable());
        assertEquals(1, result.getIsVersionable());
        assertEquals(DataType.STRING, result.getDataType());
        assertEquals(1, result.getIsNullable());
    }

    @Test
    void testAttributeNameNullReturnsNull() throws Exception {
        Attributes item = new Attributes();
        item.setAttributeName(""); // empty
        item.setRawDataType("String");
        item.setRawNullable("Yes");

        Attributes result = processor.process(item);

        assertNull(result);
        verify(validationLogRepository, times(1)).saveAll(anyList());
    }

    @Test
    void testAttributeNameHasSpacesReturnsNull() throws Exception {
        Attributes item = new Attributes();
        item.setAttributeName("Invalid Attribute Name"); // spaces
        item.setRawDataType("String");
        item.setRawNullable("Yes");

        Attributes result = processor.process(item);

        assertNull(result);
    }

    @Test
    void testAttributeNameSpecialCharsReturnsNull() throws Exception {
        Attributes item = new Attributes();
        item.setAttributeName("Attr@123"); // invalid chars
        item.setRawDataType("String");
        item.setRawNullable("Yes");

        Attributes result = processor.process(item);

        assertNull(result);
    }

    @Test
    void testDuplicateInFileReturnsNull() throws Exception {
        Attributes item1 = new Attributes();
        item1.setAttributeName("UniqueName");
        item1.setIsReclassable(1);
        item1.setIsVersionable(1);
        item1.setRawDataType("String");
        item1.setRawNullable("Yes");

        Attributes item2 = new Attributes();
        item2.setAttributeName("UniqueName"); // Same name!
        item2.setIsReclassable(1);
        item2.setIsVersionable(1);
        item2.setRawDataType("String");
        item2.setRawNullable("Yes");

        Attributes result1 = processor.process(item1);
        assertNotNull(result1);

        Attributes result2 = processor.process(item2);
        assertNull(result2, "Should return null to filter duplicate entries within file silently");
    }

    @Test
    void testDuplicateInDBReturnsNull() throws Exception {
        // Reinitialize processor setup simulating preloaded DB cache
        Attributes existingAttr = new Attributes();
        existingAttr.setAttributeName("DB_ATTRIBUTE");
        
        when(attributesRepository.findAll()).thenReturn(Collections.singletonList(existingAttr));
        
        org.springframework.batch.core.JobParameters jobParams = new org.springframework.batch.core.JobParametersBuilder()
                .addString("tenantId", "tenant123")
                .toJobParameters();
        JobExecution jobExecution = new JobExecution(102L, jobParams);
        StepExecution stepExecution = new StepExecution("attributeImportStep", jobExecution);
        processor.beforeStep(stepExecution);

        Attributes item = new Attributes();
        item.setAttributeName("DB_ATTRIBUTE"); // Matches preloaded uppercase record
        item.setIsReclassable(1);
        item.setIsVersionable(1);
        item.setRawDataType("String");
        item.setRawNullable("Yes");

        Attributes result = processor.process(item);

        assertNull(result, "Should filter out item when attribute name matches preloaded database values");
    }

    @Test
    void testInvalidReclassableBooleanReturnsNull() throws Exception {
        Attributes item = new Attributes();
        item.setAttributeName("ValidName");
        item.setIsReclassable(-1); // Invalid boolean sentinel
        item.setRawDataType("String");
        item.setRawNullable("Yes");

        Attributes result = processor.process(item);

        assertNull(result);
    }

    @Test
    void testInvalidDataTypeReturnsNull() throws Exception {
        Attributes item = new Attributes();
        item.setAttributeName("ValidName");
        item.setIsReclassable(1);
        item.setIsVersionable(1);
        item.setRawDataType("UnknownType"); // Malformed DataType
        item.setRawNullable("Yes");

        Attributes result = processor.process(item);

        assertNull(result);
    }

    @Test
    void testInvalidNullableValueReturnsNull() throws Exception {
        Attributes item = new Attributes();
        item.setAttributeName("ValidName");
        item.setIsReclassable(1);
        item.setIsVersionable(1);
        item.setRawDataType("String");
        item.setRawNullable("NotApplicable"); // Invalid nullable value

        Attributes result = processor.process(item);

        assertNull(result);
    }

    @Test
    void testReclassableTrueAndVersionableFalseLogicReturnsNull() throws Exception {
        Attributes item = new Attributes();
        item.setAttributeName("ValidName");
        item.setIsReclassable(1); // True
        item.setIsVersionable(0); // False
        item.setRawDataType("String");
        item.setRawNullable("Yes");

        Attributes result = processor.process(item);

        assertNull(result, "Should fail logical cross-field dependency: reclassable without versionable");
    }

    @Test
    void testEmptyReclassableDefaultsToTrueAndLogsWarning() throws Exception {
        Attributes item = new Attributes();
        item.setAttributeName("ValidName");
        item.setIsReclassable(-2); // Missing sentinel
        item.setIsVersionable(1);
        item.setRawDataType("String");
        item.setRawNullable("Yes");

        Attributes result = processor.process(item);

        assertNotNull(result);
        assertEquals(1, result.getIsReclassable(), "Should apply default value (true) to missing reclassable flag");
    }
}
