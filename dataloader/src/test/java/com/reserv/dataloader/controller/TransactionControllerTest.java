package com.reserv.dataloader.controller;

import com.fyntrac.common.service.TransactionService;
import com.reserv.dataloader.validation.TransactionValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.NoSuchElementException;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class TransactionControllerTest {

    private MockMvc mockMvc;

    @Mock
    private TransactionService transactionService;

    @Mock
    private TransactionValidator transactionValidator;

    @InjectMocks
    private TransactionController transactionController;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(transactionController).build();
    }

    @Test
    void testDeleteTransactionByIdSuccess() throws Exception {
        String id = "txn123";
        doNothing().when(transactionService).removeTransactionById(id);

        mockMvc.perform(delete("/api/dataloader/transaction/delete/{id}", id))
                .andExpect(status().isNoContent());

        verify(transactionService, times(1)).removeTransactionById(id);
    }

    @Test
    void testDeleteTransactionByIdFailure() throws Exception {
        String id = "txn123";
        doThrow(new RuntimeException("Database error")).when(transactionService).removeTransactionById(id);

        mockMvc.perform(delete("/api/dataloader/transaction/delete/{id}", id))
                .andExpect(status().isInternalServerError());

        verify(transactionService, times(1)).removeTransactionById(id);
    }

    @Test
    void testDeleteTransactionByNameSuccess() throws Exception {
        String name = "TransactionA";
        doNothing().when(transactionService).removeTransactionByName(name);

        mockMvc.perform(delete("/api/dataloader/transaction/delete/name/{name}", name))
                .andExpect(status().isNoContent());

        verify(transactionService, times(1)).removeTransactionByName(name);
    }

    @Test
    void testDeleteTransactionByNameNotFound() throws Exception {
        String name = "NonExistent";
        doThrow(new NoSuchElementException("Not found")).when(transactionService).removeTransactionByName(name);

        mockMvc.perform(delete("/api/dataloader/transaction/delete/name/{name}", name))
                .andExpect(status().isNotFound());

        verify(transactionService, times(1)).removeTransactionByName(name);
    }

    @Test
    void testDeleteTransactionByNameBadRequest() throws Exception {
        String name = "   ";
        doThrow(new IllegalArgumentException("Invalid name")).when(transactionService).removeTransactionByName(name);

        mockMvc.perform(delete("/api/dataloader/transaction/delete/name/{name}", name))
                .andExpect(status().isBadRequest());

        verify(transactionService, times(1)).removeTransactionByName(name);
    }
}
