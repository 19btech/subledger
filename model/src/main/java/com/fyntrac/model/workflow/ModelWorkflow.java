package com.fyntrac.model.workflow;

public interface ModelWorkflow {
    public void loadTransactions()throws Exception;
    public void loadMetrics()throws Exception;
    public void loadInstrumentAttributes()throws Exception;
    public void valuateModel()throws Exception;
}
