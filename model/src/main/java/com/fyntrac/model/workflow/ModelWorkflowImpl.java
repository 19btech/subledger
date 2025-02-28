package com.fyntrac.model.workflow;

import com.fyntrac.common.utils.ExcelUtil;
import com.fyntrac.common.utils.StringUtil;
import com.fyntrac.model.exception.LoadExcelModelExecption;

import java.util.HashMap;

public class ModelWorkflowImpl implements ModelWorkflow{
    protected ModelWorkflowContext context;

    public ModelWorkflowImpl(ModelWorkflowContext context) {
        this.context = context;
    }

    @Override
    public void loadTransactions() {

    }

    @Override
    public void loadMetrics() {

    }

    @Override
    public void loadInstrumentAttributes() {

    }

    @Override
    public void valuateModel() {

    }
}
