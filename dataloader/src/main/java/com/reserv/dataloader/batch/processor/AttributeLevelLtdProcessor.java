package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.TransactionActivity;
import org.springframework.batch.item.ItemProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AttributeLevelLtdProcessor
        implements ItemProcessor<TransactionActivity, List<Records.AttributeLevelLtdRecord>> {

    private final Map<String, Set<String>> transactionToMetrics;
    public AttributeLevelLtdProcessor(Map<String, Set<String>> transactionToMetrics) {
        this.transactionToMetrics = transactionToMetrics;
    }

    @Override
    public List<Records.AttributeLevelLtdRecord> process(TransactionActivity activity) throws Exception {
        if (activity == null || activity.getTransactionName() == null) {
            return null;
        }

        Set<String> metrics = transactionToMetrics.getOrDefault(activity.getTransactionName().toUpperCase(), Set.of());

        if (metrics.isEmpty()) {
            return null;
        }

        int postingDate = activity.getPostingDate();
        int accountingPeriod = activity.getAccountingPeriod().getPeriodId();
        List<Records.AttributeLevelLtdRecord> records = new ArrayList<>(0);
        for (String metric : metrics) {
            Records.AttributeLevelLtdRecord record = RecordFactory.createAttributeLevelLtdRecord(
                    metric,
                    activity.getInstrumentId(),
                    activity.getAttributeId(),
                    postingDate,
                    accountingPeriod,
                    activity.getAmount()
            );
            records.add(record);
        }

        return List.copyOf(records);
    }
}
