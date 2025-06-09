package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.BaseLtd;
import com.fyntrac.common.entity.InstrumentLevelLtd;
import com.fyntrac.common.entity.MetricLevelLtd;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;

public class PostAggregationMetricLevelLtdProcessor implements ItemProcessor<MetricLevelLtd, MetricLevelLtd> {

    private final Long executionDate;

    public PostAggregationMetricLevelLtdProcessor(Long executionDate) {
        this.executionDate = executionDate;
    }

    @Override
    public MetricLevelLtd process(MetricLevelLtd metricLevelLtd) {
        if (executionDate == null || executionDate == 0L) {
            throw new IllegalArgumentException("Missing job parameter: executionDate");
        }
        final MetricLevelLtd ltd = new MetricLevelLtd();
        final BigDecimal endingBalance = metricLevelLtd.getBalance().getEndingBalance();
        final BaseLtd balance = BaseLtd.builder()
                .beginningBalance(metricLevelLtd.getBalance().getEndingBalance())
                .activity(BigDecimal.valueOf(0L)).endingBalance(endingBalance).build();

        if (metricLevelLtd.getPostingDate() < executionDate) {
            ltd.setPostingDate(executionDate.intValue());
            ltd.setAccountingPeriodId(metricLevelLtd.getAccountingPeriodId());
            ltd.setMetricName(metricLevelLtd.getMetricName());
            ltd.setAccountingPeriodId(DateUtil.getAccountingPeriodId(executionDate.intValue()));
            ltd.setBalance(balance);
        }
        return ltd;
    }
}

