package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.service.TransactionService;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;

public class TransactionActivityItemProcessor implements ItemProcessor<Map<String,Object>,TransactionActivity> {
    @Override
    public TransactionActivity process(Map<String, Object> item) throws Exception {
        final TransactionActivity transactionActivity = new TransactionActivity();
        int effectiveDate = 0;
        Date transactionDate = null;
        String instrumentId = "";
        String attributeId = "";
        int postingDate = 0;
        BigDecimal amount = BigDecimal.valueOf(0L);
        String transactionName = "";

        for (Map.Entry<String, Object> entry : item.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key.equalsIgnoreCase("ACTIVITYUPLOADID")) {
                continue;
            } else if (key.equalsIgnoreCase("TRANSACTIONDATE")) {
                LocalDate localDate = LocalDate.parse((String) value, DateTimeFormatter.ofPattern("M/dd/yyyy"));
                transactionDate = Date.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());
                effectiveDate = DateUtil.dateInNumber(transactionDate);
            } else if (key.equalsIgnoreCase("INSTRUMENTID")) {
                instrumentId = (String) value;
            } else if (key.equalsIgnoreCase("ATTRIBUTEID") || key.equalsIgnoreCase("ATRRIBUTEID")) {
                attributeId = (String) value;
            } else if (key.equalsIgnoreCase("POSTINGDATE")) {
                Date pDate = DateUtil.parseDate((String) value);
                postingDate = DateUtil.dateInNumber(pDate);
            } else if (key.equalsIgnoreCase("AMOUNT")) {
                double amnt = Double.valueOf((String) value);
                amount = BigDecimal.valueOf(amnt);
            } else if (key.equalsIgnoreCase("TRANSACTIONNAME") || key.equalsIgnoreCase("TRANSACTIONTYPE")) {
                transactionName = (String) value;
            }
        }

        assert transactionDate != null;
        Instant instant = transactionDate.toInstant();
        LocalDateTime localDateTime = instant.atZone(ZoneId.of("UTC")).toLocalDateTime();
        return TransactionActivity.builder().effectiveDate(effectiveDate)
                .transactionName(transactionName)
        .instrumentId(instrumentId)
                .transactionDate(Date.from(localDateTime.atZone(ZoneId.of("UTC")).toInstant()))
                .amount(amount)
                .attributeId(attributeId)
        .source(Source.ETL)
        .postingDate(postingDate).build();

    }
}
