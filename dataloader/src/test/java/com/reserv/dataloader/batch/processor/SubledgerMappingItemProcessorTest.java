package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.entity.SubledgerMapping;
import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.enums.AccountType;
import com.fyntrac.common.enums.EntryType;
import com.fyntrac.common.enums.Sign;
import com.fyntrac.common.repository.AccountTypesRepository;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import com.fyntrac.common.repository.SubledgerMappingRepository;
import com.fyntrac.common.repository.TransactionsRepository;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.validation.SubledgerMappingValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SubledgerMappingItemProcessorTest {

    @Mock
    private TransactionsRepository transactionsRepository;
    @Mock
    private AccountTypesRepository accountTypesRepository;
    @Mock
    private SubledgerMappingRepository subledgerMappingRepository;
    @Mock
    private RefDataValidationLogRepository validationLogRepository;

    private SubledgerMappingValidator validator;
    private SubledgerMappingItemProcessor processor;

    @BeforeEach
    void setUp() {
        validator = new SubledgerMappingValidator();
        processor = new SubledgerMappingItemProcessor(
                validator, transactionsRepository, accountTypesRepository, subledgerMappingRepository, validationLogRepository);
    }

    private void initStep(String tenantId, JobExecution jobExecution) {
        StepExecution stepExecution = new StepExecution("subledgerMappingImportStep", jobExecution);
        processor.beforeStep(stepExecution);
    }

    @Test
    void testValidMappingPassesSuccessfully() throws Exception {
        when(transactionsRepository.findAll()).thenReturn(Collections.singletonList(txn("ValidTxn")));
        when(accountTypesRepository.findAll()).thenReturn(Collections.singletonList(accType("SubTypeA")));
        when(subledgerMappingRepository.findAll()).thenReturn(Collections.emptyList());

        initStep("tenant123", new JobExecution(301L, params("tenant123")));

        SubledgerMapping item = mapping("ValidTxn", Sign.POSITIVE, EntryType.DEBIT, "SubTypeA");

        SubledgerMapping result = processor.process(item);
        assertNotNull(result);
    }

    @Test
    void testExactDuplicateAlreadyInDbIsCaughtOnReUpload() throws Exception {
        // Simulate the bug scenario: the same mapping file is uploaded a second time,
        // so this exact (transactionName, sign, entryType, accountSubType) combination
        // is already persisted from a prior job run.
        when(transactionsRepository.findAll()).thenReturn(Collections.singletonList(txn("ValidTxn")));
        when(accountTypesRepository.findAll()).thenReturn(Collections.singletonList(accType("SubTypeA")));
        when(subledgerMappingRepository.findAll())
                .thenReturn(Collections.singletonList(mapping("ValidTxn", Sign.POSITIVE, EntryType.DEBIT, "SubTypeA")));

        initStep("tenant123", new JobExecution(302L, params("tenant123")));

        SubledgerMapping item = mapping("ValidTxn", Sign.POSITIVE, EntryType.DEBIT, "SubTypeA");

        ItemValidationException ex = assertThrows(ItemValidationException.class, () -> processor.process(item));
        assertTrue(ex.getValidationErrors().stream().anyMatch(e -> "ERROR".equals(e.getSeverity())));
    }

    private static Transactions txn(String name) {
        Transactions t = new Transactions();
        t.setName(name);
        return t;
    }

    private static AccountTypes accType(String subType) {
        AccountTypes a = new AccountTypes();
        a.setAccountSubType(subType);
        a.setAccountType(AccountType.BALANCESHEET);
        return a;
    }

    private static SubledgerMapping mapping(String txnName, Sign sign, EntryType entryType, String subType) {
        SubledgerMapping m = new SubledgerMapping();
        m.setTransactionName(txnName);
        m.setSign(sign);
        m.setEntryType(entryType);
        m.setAccountSubType(subType);
        return m;
    }

    private static org.springframework.batch.core.JobParameters params(String tenantId) {
        return new JobParametersBuilder().addString("tenantId", tenantId).toJobParameters();
    }
}
