package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.AttributeLevelLtd;
import com.fyntrac.common.entity.BaseLtd;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

public class AttributeBalanceItemProcessor implements ItemProcessor<Map<String, Object>, AttributeLevelLtd> {

    @Override
    public AttributeLevelLtd process(Map<String, Object> item) throws Exception {

        String instrumentId = "";
        String attributeId = "";
        int intPostingDate = 0;
        Date postingDate = null;
        String metricName = "";

        BigDecimal beginningBalance = BigDecimal.ZERO;
        BigDecimal activityAmount = BigDecimal.ZERO;
        BigDecimal endingBalance = BigDecimal.ZERO;

        for (Map.Entry<String, Object> entry : item.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.isBlank()) continue;

            Object value = entry.getValue();
            if (value == null) continue;

            switch (key.toUpperCase()) {
                case "ACTIVITYUPLOADID" -> {
                    // Skip this field
                }
                case "METRICNAME", "METRIC" -> {
                    if (value instanceof String strVal) {
                        metricName = strVal;
                    } else {
                        metricName = String.valueOf(value);
                    }
                }
                case "INSTRUMENTID" -> instrumentId = String.valueOf(value);

                case "ATTRIBUTEID" -> attributeId = String.valueOf(value);

                case "POSTINGDATE" -> {
                    if (value instanceof String strDate) {
                        postingDate = DateUtil.parseDate(strDate);
                        intPostingDate = DateUtil.dateInNumber(postingDate);
                    } else if (value instanceof Date dt) {
                        postingDate = dt;
                        intPostingDate = DateUtil.dateInNumber(postingDate);
                    }
                }
                case "ACTIVITYAMOUNT", "ACTIVITY" -> activityAmount = toBigDecimal(value);

                case "BEGINNINGBALANCE" -> beginningBalance = toBigDecimal(value);

                case "ENDINGBALANCE" -> endingBalance = toBigDecimal(value);

                default -> {
                    // ignore unknown fields
                }
            }

        }

        int periodId = com.fyntrac.common.utils.DateUtil.getAccountingPeriodId(postingDate);

        BaseLtd balance = BaseLtd.builder()
                .beginningBalance(beginningBalance)
                .activity(activityAmount)
                .endingBalance(endingBalance)
                .build();

        return AttributeLevelLtd.builder()
                .attributeId(attributeId)
                .instrumentId(instrumentId)
                .postingDate(intPostingDate)
                .accountingPeriodId(periodId)
                .metricName(metricName)
                .balance(balance)
                .build();
    }

    /**
     * Safely convert an Object to BigDecimal
     */
    private static BigDecimal toBigDecimal(Object value) {
        if (value == null) return BigDecimal.ZERO;
        if (value instanceof BigDecimal bd) return bd;
        if (value instanceof Number num) return BigDecimal.valueOf(num.doubleValue());
        try {
            return new BigDecimal(value.toString());
        } catch (NumberFormatException e) {
            return BigDecimal.ZERO;
        }
    }

}
