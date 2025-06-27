package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.component.TransactionActivityQueue;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.service.TransactionActivityReversalService;
import com.fyntrac.common.service.TransactionService;
import org.springframework.batch.item.ItemProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ReversalActivityProcessor implements ItemProcessor<Records.InstrumentReplayRecord, List<TransactionActivity>> {

    private final TransactionActivityReversalService reversalService;
    private final TransactionActivityQueue transactionActivityQueue;
    private final TransactionService transactionService;

    private final String tenantId;
    private final Long jobId;

    public ReversalActivityProcessor(String tenantId,Long jobId
            , TransactionActivityReversalService reversalService
            , TransactionActivityQueue transactionActivityQueue
    , TransactionService transactionService) {
        this.tenantId = tenantId;
        this.jobId = jobId;
        this.reversalService = reversalService;
        this.transactionActivityQueue = transactionActivityQueue;
        this.transactionService = transactionService;
    }

    @Override
    public List<TransactionActivity> process(Records.InstrumentReplayRecord record) {
        List<TransactionActivity> result = new ArrayList<>();
        reversalService.getDataService().setTenantId(tenantId);
        Collection<Records.TransactionActivityReversalRecord> reversals =
                reversalService.getGroupTransactionSummary(record.instrumentId(), record.postingDate(), record.effectiveDate());

        for (Records.TransactionActivityReversalRecord reversal : reversals) {
            TransactionActivity activity = reversalService.generateTransactionReversalActivity(reversal, record);
            Transactions transaction = this.transactionService.getTransaction(activity.getTransactionName().toUpperCase());
            activity.setIsReplayable(transaction.getIsReplayable());
            result.add(activity);
            transactionActivityQueue.add(tenantId, jobId, activity);
        }
        System.out.println("PROCESSOR: Processing instrument " + record.instrumentId());
        System.out.println("PROCESSOR: Processing instrument Size " + result.size());
        return result;
    }
}
