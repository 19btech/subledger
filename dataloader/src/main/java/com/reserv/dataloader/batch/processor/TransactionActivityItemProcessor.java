package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.batch.item.ItemProcessor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class TransactionActivityItemProcessor implements ItemProcessor<TransactionActivity,TransactionActivity> {
    @Override
    public TransactionActivity process(TransactionActivity item) throws Exception {
        final TransactionActivity transactionActivity = new TransactionActivity();
        transactionActivity.setTransactionName(item.getTransactionName());
        transactionActivity.setInstrumentId(item.getInstrumentId());
        Instant instant = item.getTransactionDate().toInstant();
        LocalDateTime localDateTime = instant.atZone(ZoneId.of("UTC")).toLocalDateTime();
        // Process the date as needed
        // For example, you can set it back to the item or modify it
        transactionActivity.setTransactionDate(Date.from(localDateTime.atZone(ZoneId.of("UTC")).toInstant()));
        transactionActivity.setAmount(item.getAmount());
        transactionActivity.setAttributeId(item.getAttributeId());
        transactionActivity.setSource(Source.ETL);
        transactionActivity.setPostingDate(item.getPostingDate());
        int effectiveDateInteger = DateUtil.dateInNumber(item.getTransactionDate());
        transactionActivity.setEffectiveDate(effectiveDateInteger);
        return transactionActivity;
    }

}
