package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.batch.item.ItemProcessor;

public class TransactionActivityItemProcessor implements ItemProcessor<TransactionActivity,TransactionActivity> {
    @Override
    public TransactionActivity process(TransactionActivity item) throws Exception {
        final TransactionActivity transactionActivity = new TransactionActivity();
        transactionActivity.setTransactionName(item.getTransactionName());
        transactionActivity.setInstrumentId(item.getInstrumentId());
        transactionActivity.setTransactionDate(DateUtil.stripTime(item.getTransactionDate()));
        transactionActivity.setAmount(item.getAmount());
        transactionActivity.setAttributeId(item.getAttributeId());
        transactionActivity.setSource(Source.ETL);
        return transactionActivity;
    }

}
