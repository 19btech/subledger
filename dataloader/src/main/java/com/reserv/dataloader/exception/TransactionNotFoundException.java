package com.reserv.dataloader.exception;

public class TransactionNotFoundException extends Exception {
    public TransactionNotFoundException(String errorMessage) {
        super(errorMessage);
    }
}