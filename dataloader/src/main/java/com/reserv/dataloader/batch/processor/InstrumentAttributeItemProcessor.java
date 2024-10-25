package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.batch.item.ItemProcessor;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class InstrumentAttributeItemProcessor implements ItemProcessor<Map<String,Object>,InstrumentAttribute> {
    @Override
    public InstrumentAttribute process(Map<String, Object> item) throws Exception {
        final InstrumentAttribute instrumentAttribute = new InstrumentAttribute();
        final Map<String, Object> attributes = new HashMap<>();
        for (Map.Entry<String, Object> entry : item.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if(key.equalsIgnoreCase("ACTIVITYUPLOADID")){
                continue;
            } else if (key.equalsIgnoreCase("EFFECTIVEDATE")) {
                Date date = DateUtil.parseDate((String) value);
                instrumentAttribute.setEffectiveDate(date);
            } else if (key.equalsIgnoreCase("INSTRUMENTID")) {
                instrumentAttribute.setInstrumentId((String) value);
            } else if (key.equalsIgnoreCase("ATTRIBUTEID")) {
                instrumentAttribute.setAttributeId((String) value);
            } else {
                attributes.put(key, value);
            }
        }
        instrumentAttribute.setAttributes(attributes);
        return instrumentAttribute;
    }
}
