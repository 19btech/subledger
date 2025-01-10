package com.reserv.dataloader.exception;

public class AccountingPeriodClosedException extends Exception {
    public AccountingPeriodClosedException(String errorMessage) {
        super(errorMessage);
    }
}
