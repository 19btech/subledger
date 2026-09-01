package com.reserv.dataloader.controller;

import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.entity.ChartOfAccount;
import com.fyntrac.common.enums.DataType;
import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.repository.AccountTypesRepository;
import com.fyntrac.common.repository.AttributesRepository;
import com.fyntrac.common.service.DataService;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChartOfAccountControllerTest {

    @Mock
    private DataService dataService;

    @Mock
    private AccountTypesRepository accountTypesRepository;

    @Mock
    private AttributesRepository attributesRepository;

    @InjectMocks
    private ChartOfAccountController controller;

    @BeforeEach
    void setUp() {
        AccountTypes assetType = new AccountTypes();
        assetType.setAccountSubType("ASSET");
        lenient().when(accountTypesRepository.findAll()).thenReturn(List.of(assetType));
        lenient().when(attributesRepository.findAll()).thenReturn(List.of());
    }

    private ChartOfAccount account(String id, String number, String name, String subtype, Map<String, Object> attrs) {
        ChartOfAccount coa = new ChartOfAccount();
        coa.setId(id);
        coa.setAccountNumber(number);
        coa.setAccountName(name);
        coa.setAccountSubtype(subtype);
        coa.setAttributes(attrs);
        return coa;
    }

    @Test
    void saveDate_ExactDuplicate_ReturnsBadRequestErrDup01() {
        ChartOfAccount existing = account("existing-id", "ACC_001", "Cash at Bank", "ASSET", new HashMap<>());
        when(dataService.fetchAllData(ChartOfAccount.class)).thenReturn(List.of(existing));

        ChartOfAccount incoming = account(null, "ACC_001", "Cash at Bank", "ASSET", new HashMap<>());

        ResponseEntity<?> response = controller.saveDate(incoming);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        @SuppressWarnings("unchecked")
        List<ItemValidationException.ValidationError> errors = (List<ItemValidationException.ValidationError>) response.getBody();
        assertNotNull(errors);
        assertTrue(errors.stream().anyMatch(e -> ErrorCode.ERR_DUP_01.getCode().equals(e.getErrorCode())));
    }

    @Test
    void saveDate_SameSubtypeAttrsDifferentNumber_ReturnsBadRequestErrDup02() {
        ChartOfAccount existing = account("existing-id", "ACC_001", "Cash at Bank", "ASSET", new HashMap<>());
        when(dataService.fetchAllData(ChartOfAccount.class)).thenReturn(List.of(existing));

        ChartOfAccount incoming = account(null, "ACC_002", "Different Name", "ASSET", new HashMap<>());

        ResponseEntity<?> response = controller.saveDate(incoming);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        @SuppressWarnings("unchecked")
        List<ItemValidationException.ValidationError> errors = (List<ItemValidationException.ValidationError>) response.getBody();
        assertNotNull(errors);
        assertTrue(errors.stream().anyMatch(e -> ErrorCode.ERR_DUP_02.getCode().equals(e.getErrorCode())));
    }

    @Test
    void saveDate_EditExcludesSelf_Passes() {
        ChartOfAccount existing = account("same-id", "ACC_001", "Cash at Bank", "ASSET", new HashMap<>());
        when(dataService.fetchAllData(ChartOfAccount.class)).thenReturn(List.of(existing));

        ChartOfAccount incoming = account("same-id", "ACC_001", "Cash at Bank", "ASSET", new HashMap<>());

        ResponseEntity<?> response = controller.saveDate(incoming);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        verify(dataService).save(incoming);
    }

    @Test
    void saveDate_NoConflicts_SavesSuccessfully() {
        when(dataService.fetchAllData(ChartOfAccount.class)).thenReturn(List.of());

        ChartOfAccount incoming = account(null, "ACC_003", "New Account", "ASSET", new HashMap<>());

        ResponseEntity<?> response = controller.saveDate(incoming);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        verify(dataService).save(incoming);
    }
}
