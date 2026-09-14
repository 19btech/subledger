package com.reserv.dataloader.service.upload;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.enums.AccountingRules;
import com.fyntrac.common.repository.CustomTableDefinitionRepository;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.ExecutionStateService;
import com.reserv.dataloader.exception.MultiplePostingDatesException;
import com.reserv.dataloader.service.model.ModelExecutionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FileUploadServiceTest {

    @Mock
    private TenantContextHolder tenantContextHolder;
    @Mock
    private ActivityUploadService activityUploadService;
    @Mock
    private TransactionsUploadService transactionsUploadService;
    @Mock
    private CustomTableDefinitionRepository customTableDefinitionRepository;
    @Mock
    private ExecutionStateService executionStateService;
    @Mock
    private AccountingPeriodService accountingPeriodService;
    @Mock
    private MongoTemplate mongoTemplate;
    @Mock
    private ModelExecutionService modelExecutionService;

    private FileUploadService fileUploadService;

    @BeforeEach
    void setUp() {
        fileUploadService = new FileUploadService(
                tenantContextHolder,
                activityUploadService,
                transactionsUploadService,
                customTableDefinitionRepository,
                executionStateService,
                accountingPeriodService,
                mongoTemplate,
                modelExecutionService
        );
    }

    private void writeCsvFile(Path path, String content) throws IOException {
        Files.writeString(path, content);
    }

    @Test
    void testValidateActivityFiles_Success(@TempDir Path tempDir) throws Exception {
        Path file1 = tempDir.resolve("transactionactivity.csv");
        writeCsvFile(file1, "POSTINGDATE,OTHER_COL\n06/05/2026,value1\n2026-06-05,value2\n20260605,value3");

        ExecutionState state = new ExecutionState();
        state.setExecutionDate(20260605);
        when(executionStateService.getExecutionState()).thenReturn(state);

        Map<AccountingRules, String> activityMap = new HashMap<>();
        activityMap.put(AccountingRules.TRANSACTIONACTIVITY, file1.toAbsolutePath().toString());

        Integer result = fileUploadService.validateActivityFiles(activityMap);
        assertEquals(20260605, result);
    }

    @Test
    void testValidateActivityFiles_IllegalArgumentException_EarlierPostingDate(@TempDir Path tempDir) throws Exception {
        Path file1 = tempDir.resolve("transactionactivity.csv");
        writeCsvFile(file1, "POSTINGDATE,OTHER_COL\n06/04/2026,value1\n");

        ExecutionState state = new ExecutionState();
        state.setExecutionDate(20260605);
        when(executionStateService.getExecutionState()).thenReturn(state);

        Map<AccountingRules, String> activityMap = new HashMap<>();
        activityMap.put(AccountingRules.TRANSACTIONACTIVITY, file1.toAbsolutePath().toString());

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            fileUploadService.validateActivityFiles(activityMap);
        });
        assertTrue(ex.getMessage().contains("is earlier than the current executionDate"));
    }

    @Test
    void testValidateActivityFiles_MultiplePostingDatesException(@TempDir Path tempDir) throws Exception {
        Path file1 = tempDir.resolve("transactionactivity.csv");
        writeCsvFile(file1, "POSTINGDATE,OTHER_COL\n06/05/2026,value1\n06/06/2026,value2\n");

        ExecutionState state = new ExecutionState();
        state.setExecutionDate(20260605);
        when(executionStateService.getExecutionState()).thenReturn(state);

        Map<AccountingRules, String> activityMap = new HashMap<>();
        activityMap.put(AccountingRules.TRANSACTIONACTIVITY, file1.toAbsolutePath().toString());

        MultiplePostingDatesException ex = assertThrows(MultiplePostingDatesException.class, () -> {
            fileUploadService.validateActivityFiles(activityMap);
        });
        assertTrue(ex.getMessage().contains("Multiple posting dates are not allowed"));
    }

    @Test
    void testValidateActivityFiles_Success_NullExecutionState(@TempDir Path tempDir) throws Exception {
        Path file1 = tempDir.resolve("transactionactivity.csv");
        writeCsvFile(file1, "POSTINGDATE,OTHER_COL\n06/05/2026,value1\n2026-06-05,value2");

        when(executionStateService.getExecutionState()).thenReturn(null);

        Map<AccountingRules, String> activityMap = new HashMap<>();
        activityMap.put(AccountingRules.TRANSACTIONACTIVITY, file1.toAbsolutePath().toString());

        Integer result = fileUploadService.validateActivityFiles(activityMap);
        assertEquals(20260605, result);
    }

    @Test
    void testValidateActivityFiles_MultiplePostingDates_NullExecutionState(@TempDir Path tempDir) throws Exception {
        Path file1 = tempDir.resolve("transactionactivity.csv");
        writeCsvFile(file1, "POSTINGDATE,OTHER_COL\n06/05/2026,value1\n06/06/2026,value2");

        when(executionStateService.getExecutionState()).thenReturn(null);

        Map<AccountingRules, String> activityMap = new HashMap<>();
        activityMap.put(AccountingRules.TRANSACTIONACTIVITY, file1.toAbsolutePath().toString());

        assertThrows(MultiplePostingDatesException.class, () -> {
            fileUploadService.validateActivityFiles(activityMap);
        });
    }

    @Test
    void testValidateActivityFiles_MultipleFiles_Success(@TempDir Path tempDir) throws Exception {
        Path file1 = tempDir.resolve("transactionactivity.csv");
        writeCsvFile(file1, "POSTINGDATE,OTHER_COL\n06/05/2026,value1");

        Path file2 = tempDir.resolve("instrumentattribute.csv");
        writeCsvFile(file2, "POSTINGDATE,OTHER_COL\n20260605,value2");

        ExecutionState state = new ExecutionState();
        state.setExecutionDate(20260605);
        when(executionStateService.getExecutionState()).thenReturn(state);

        Map<AccountingRules, String> activityMap = new HashMap<>();
        activityMap.put(AccountingRules.TRANSACTIONACTIVITY, file1.toAbsolutePath().toString());
        activityMap.put(AccountingRules.INSTRUMENTATTRIBUTE, file2.toAbsolutePath().toString());

        Integer result = fileUploadService.validateActivityFiles(activityMap);
        assertEquals(20260605, result);
    }

    @Test
    void testValidateActivityFiles_MultipleFiles_DifferentPostingDates(@TempDir Path tempDir) throws Exception {
        Path file1 = tempDir.resolve("transactionactivity.csv");
        writeCsvFile(file1, "POSTINGDATE,OTHER_COL\n06/05/2026,value1");

        Path file2 = tempDir.resolve("instrumentattribute.csv");
        writeCsvFile(file2, "POSTINGDATE,OTHER_COL\n20260606,value2");

        ExecutionState state = new ExecutionState();
        state.setExecutionDate(20260605);
        when(executionStateService.getExecutionState()).thenReturn(state);

        Map<AccountingRules, String> activityMap = new HashMap<>();
        activityMap.put(AccountingRules.TRANSACTIONACTIVITY, file1.toAbsolutePath().toString());
        activityMap.put(AccountingRules.INSTRUMENTATTRIBUTE, file2.toAbsolutePath().toString());

        MultiplePostingDatesException ex = assertThrows(MultiplePostingDatesException.class, () -> {
            fileUploadService.validateActivityFiles(activityMap);
        });
        assertTrue(ex.getMessage().contains("Multiple different posting dates detected across activity files"));
    }
}
