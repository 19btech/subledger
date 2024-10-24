package com.reserv.dataloader.batch.processor;


import com.fyntrac.common.entity.ChartOfAccount;
import org.springframework.batch.item.ItemProcessor;

import java.util.HashMap;
import java.util.Map;

public class ChartOfAccountItemProcessor implements ItemProcessor<Map<String,Object>, ChartOfAccount> {
    @Override
    public ChartOfAccount process(Map<String, Object> item) throws Exception {
        final ChartOfAccount chartOfAccount = new ChartOfAccount();
        final Map<String, Object> attributes = new HashMap<>();
        for (Map.Entry<String, Object> entry : item.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if(key.equalsIgnoreCase("ACTIVITYUPLOADID")){
                continue;
            } else if (key.equalsIgnoreCase("ACCOUNTNUMBER")) {
                chartOfAccount.setAccountNumber((String) value);
            } else if (key.equalsIgnoreCase("ACCOUNTNAME")) {
                chartOfAccount.setAccountName((String) value);
            } else if (key.equalsIgnoreCase("ACCOUNTSUBTYPE")) {
                chartOfAccount.setAccountSubtype((String) value);
            } else {
                attributes.put(key, value);
            }
        }
        chartOfAccount.setAttributes(attributes);
        return chartOfAccount;
    }
}



