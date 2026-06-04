package com.reserv.dataloader.service.model.workflow;

import com.fyntrac.common.entity.AccountingPeriod;
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
        Integer accountingPeriodId = DateUtil.getAccountingPeriodId(postingDate);
        AccountingPeriod accountingPeriod =  accountingPeriodService.getAccountingPeriod( accountingPeriodId, tenant);
        if(accountingPeriod.getStatus() == 1) {
            throw new AccountingPeriodClosedException("Acconting Period [" + accountingPeriod.getPeriod() + "] is closed , you can not execute model on a closed accounting period.");
        }

        if ("DSL".equalsIgnoreCase(modelType)) {
            dslExecutionWorkflow.executeWorkflow(tenant, postingDate);
        } else if ("EXCEL".equalsIgnoreCase(modelType)) {
            excelExecutionWorkflow.executeWorkflow(tenant, postingDate);
        } else {
            throw new UnsupportedOperationException("Workflow for modelType " + modelType + " is not yet implemented in the new architecture.");
        }
    }
}
