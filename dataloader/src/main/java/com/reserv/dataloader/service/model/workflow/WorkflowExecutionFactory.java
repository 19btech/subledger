package com.reserv.dataloader.service.model.workflow;

import org.springframework.stereotype.Service;

@Service
public class WorkflowExecutionFactory {

    private final DslExecutionWorkflow dslExecutionWorkflow;
    private final ExcelExecutionWorkflow excelExecutionWorkflow;

    public WorkflowExecutionFactory(DslExecutionWorkflow dslExecutionWorkflow,
                                    ExcelExecutionWorkflow excelExecutionWorkflow) {
        this.dslExecutionWorkflow = dslExecutionWorkflow;
        this.excelExecutionWorkflow = excelExecutionWorkflow;
    }

    public void execute(String modelType, String tenant, int postingDate) throws Throwable {
        if ("DSL".equalsIgnoreCase(modelType)) {
            dslExecutionWorkflow.executeWorkflow(tenant, postingDate);
        } else if ("EXCEL".equalsIgnoreCase(modelType)) {
            excelExecutionWorkflow.executeWorkflow(tenant, postingDate);
        } else {
            throw new UnsupportedOperationException("Workflow for modelType " + modelType + " is not yet implemented in the new architecture.");
        }
    }
}
