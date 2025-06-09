package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.AttributeLevelLtd;
import com.fyntrac.common.entity.BaseLtd;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;

public class PostAggregationAttributeLevelLtdProcessor implements ItemProcessor<AttributeLevelLtd, AttributeLevelLtd> {

    private final Long executionDate;

    public PostAggregationAttributeLevelLtdProcessor(Long executionDate) {
        this.executionDate = executionDate;
    }

    @Override
    public AttributeLevelLtd process(AttributeLevelLtd attributeLevelLtd) {
        if (executionDate == null || executionDate == 0L) {
            throw new IllegalArgumentException("Missing job parameter: executionDate");
        }
        final AttributeLevelLtd ltd = new AttributeLevelLtd();
        final BigDecimal endingBalance = attributeLevelLtd.getBalance().getEndingBalance();
        final BaseLtd balance = BaseLtd.builder()
                .beginningBalance(attributeLevelLtd.getBalance().getEndingBalance())
                .activity(BigDecimal.valueOf(0L)).endingBalance(endingBalance).build();

        if (attributeLevelLtd.getPostingDate() < executionDate) {
            ltd.setPostingDate(executionDate.intValue());
            ltd.setAttributeId(attributeLevelLtd.getAttributeId());
            ltd.setInstrumentId(attributeLevelLtd.getInstrumentId());
            ltd.setAccountingPeriodId(attributeLevelLtd.getAccountingPeriodId());
            ltd.setMetricName(attributeLevelLtd.getMetricName());
            ltd.setAccountingPeriodId(DateUtil.getAccountingPeriodId(executionDate.intValue()));
            ltd.setBalance(balance);
        }
        return ltd;
    }
}
