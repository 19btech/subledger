package com.fyntrac.gl.service;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public abstract class BaseGeneralLedgerService {
    protected void initialize(Map<String, Object> executionContext) {

    }

    public void execute(Map<String, Object> executionContext) throws ExecutionException, InterruptedException {
        initialize(executionContext);
        perform(executionContext);
        conclude(executionContext);
    }

    protected void conclude(Map<String, Object> executionContext) throws ExecutionException, InterruptedException {

    }

    protected void perform(Map<String, Object> executionContext) throws ExecutionException, InterruptedException {

    }
}
