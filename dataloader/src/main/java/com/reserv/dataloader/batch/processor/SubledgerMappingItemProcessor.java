package com.reserv.dataloader.batch.processor;

import com.reserv.dataloader.entity.SubledgerMapping;
import org.springframework.batch.item.ItemProcessor;

public class SubledgerMappingItemProcessor implements ItemProcessor<SubledgerMapping,SubledgerMapping> {
    @Override
    public SubledgerMapping process(SubledgerMapping item) throws Exception {
        final SubledgerMapping subledgerMapping = new SubledgerMapping();
        subledgerMapping.setEntryType(item.getEntryType());
        subledgerMapping.setSign(item.getSign());
        subledgerMapping.setAccountSubType(item.getAccountSubType());
        subledgerMapping.setTransactionName(item.getTransactionName());
        return subledgerMapping;
    }
}
