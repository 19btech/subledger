package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.TransactionActivity;
import org.springframework.batch.item.ItemProcessor;

public class TransactionActivityItemProcessor implements ItemProcessor<TransactionActivity,TransactionActivity> {
    @Override
    public TransactionActivity process(TransactionActivity item) throws Exception {
        final TransactionActivity transactionActivity = new TransactionActivity();
        transactionActivity.setTransactionName(item.getTransactionName());
        transactionActivity.setInstrumentId(item.getInstrumentId());
        transactionActivity.setTransactionDate(item.getTransactionDate());
        transactionActivity.setValue(item.getValue());
        transactionActivity.setAttributeId(item.getAttributeId());
        return transactionActivity;
    }

}
