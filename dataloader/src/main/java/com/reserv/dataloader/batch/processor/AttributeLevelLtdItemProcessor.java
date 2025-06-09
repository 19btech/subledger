package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.AttributeLevelLtd;
import com.fyntrac.common.entity.Attributes;
import org.springframework.batch.item.ItemProcessor;

public class AttributeLevelLtdItemProcessor implements ItemProcessor<AttributeLevelLtd,AttributeLevelLtd> {

    @Override
    public AttributeLevelLtd process(AttributeLevelLtd item) throws Exception {
        final AttributeLevelLtd attributeLevelLtd = new AttributeLevelLtd();
        attributeLevelLtd.setAttributeId(item.getAttributeId());
        attributeLevelLtd.setMetricName(item.getMetricName());
        attributeLevelLtd.setInstrumentId(item.getInstrumentId());
        attributeLevelLtd.setPostingDate(item.getPostingDate());
        attributeLevelLtd.setAccountingPeriodId(item.getAccountingPeriodId());
        attributeLevelLtd.setBalance(item.getBalance());
        return attributeLevelLtd;
    }
}
