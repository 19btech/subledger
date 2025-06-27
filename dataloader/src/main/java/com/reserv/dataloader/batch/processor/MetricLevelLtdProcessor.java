package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.TransactionActivity;
import org.springframework.batch.item.ItemProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetricLevelLtdProcessor
        implements ItemProcessor<TransactionActivity, List<Records.MetricLevelLtdRecord>> {

    private final Map<String, Set<String>> transactionToMetrics;
    private String tenantId;
    private long jobId;
    public MetricLevelLtdProcessor(String tenantId,
                                       long jobId,
                                       Map<String, Set<String>> transactionToMetrics) {
        this.transactionToMetrics = transactionToMetrics;
        this.tenantId = tenantId;
        this.jobId = jobId;
    }

    @Override
    public List<Records.MetricLevelLtdRecord> process(TransactionActivity activity) throws Exception {
        if (activity == null || activity.getTransactionName() == null) {
            return null;
        }

        Set<String> metrics = transactionToMetrics.getOrDefault(activity.getTransactionName().toUpperCase(), Set.of());

        if (metrics.isEmpty()) {
            return null;
        }

        int postingDate = activity.getPostingDate();
        int accountingPeriod = activity.getAccountingPeriod().getPeriodId();
        List<Records.MetricLevelLtdRecord> records = new ArrayList<>(0);
        for (String metric : metrics) {
            Records.MetricLevelLtdRecord record = RecordFactory.createMetricLevelLtdRecord(
                    metric,
                    postingDate,
                    accountingPeriod,
                    activity.getAmount()
            );
            records.add(record);
        }

        return records;
    }
}
