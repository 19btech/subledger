package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.Transactions;
import org.springframework.batch.item.ItemProcessor;

public class TransactionsItemProcessor implements ItemProcessor<Transactions,Transactions> {
    @Override
    public Transactions process(Transactions item) throws Exception {
        final Transactions transaction = new Transactions();
        transaction.setName(item.getName());
        transaction.setExclusive(item.getExclusive());
        transaction.setIsGL(item.getIsGL());
        return transaction;
    }
}
