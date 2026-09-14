package com.reserv.dataloader.exception;

public class MultiplePostingDatesException extends IllegalArgumentException {
    public MultiplePostingDatesException(String errorMessage) {
        super(errorMessage);
    }
}
