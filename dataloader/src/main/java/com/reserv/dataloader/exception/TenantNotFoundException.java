package com.reserv.dataloader.exception;

import jakarta.servlet.ServletException;

public class TenantNotFoundException extends ServletException {
    public TenantNotFoundException(String errorMessage) {
        super(errorMessage);
    }
}

