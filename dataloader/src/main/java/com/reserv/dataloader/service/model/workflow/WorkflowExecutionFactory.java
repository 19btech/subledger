package com.reserv.dataloader.service.model.workflow;

import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.ExecutionInstance;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.NumberUtil;
import com.reserv.dataloader.exception.AccountingPeriodClosedException;
import org.springframework.stereotype.Service;

@Service
public class WorkflowExecutionFactory {

    private final DslExecutionWorkflow dslExecutionWorkflow;
    private final ExcelExecutionWorkflow excelExecutionWorkflow;
    private final AccountingPeriodService accountingPeriodService;

    public WorkflowExecutionFactory(DslExecutionWorkflow dslExecutionWorkflow,
                                    ExcelExecutionWorkflow excelExecutionWorkflow,
                                    AccountingPeriodService accountingPeriodService) {
        this.dslExecutionWorkflow = dslExecutionWorkflow;
        this.excelExecutionWorkflow = excelExecutionWorkflow;
        this.accountingPeriodService = accountingPeriodService;
    }

    public void execute(String modelType, String tenant, int postingDate) throws Throwable {
        rejectClosedPeriod(tenant, postingDate);

        if ("DSL".equalsIgnoreCase(modelType)) {
            dslExecutionWorkflow.executeWorkflow(tenant, postingDate);
        } else if ("EXCEL".equalsIgnoreCase(modelType)) {
            excelExecutionWorkflow.executeWorkflow(tenant, postingDate);
        } else {
            throw new UnsupportedOperationException("Workflow for modelType " + modelType + " is not yet implemented in the new architecture.");
        }
    }

    /**
     * Starts a DSL run under the given id and returns its record. With distributed runs enabled it
     * returns once the run is handed out to the dataloader pods; otherwise once it has finished.
     */
    public ExecutionInstance start(String modelType, String tenant, int postingDate, String runId) throws Throwable {
        if (!"DSL".equalsIgnoreCase(modelType)) {
            throw new UnsupportedOperationException("start(runId) is only implemented for DSL runs, not " + modelType);
        }
        rejectClosedPeriod(tenant, postingDate);
        return dslExecutionWorkflow.startWorkflow(tenant, postingDate, runId);
    }

    private void rejectClosedPeriod(String tenant, int postingDate) throws AccountingPeriodClosedException {
        Integer accountingPeriodId = DateUtil.getAccountingPeriodId(postingDate);
        AccountingPeriod accountingPeriod =  accountingPeriodService.getAccountingPeriod( accountingPeriodId, tenant, Boolean.TRUE);
        if(accountingPeriod.getStatus() == 1) {
            throw new AccountingPeriodClosedException("Acconting Period [" + accountingPeriod.getPeriod() + "] is closed , you can not execute model on a closed accounting period.");
        }
    }
}
