package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class InstrumentAttributeItemProcessor implements ItemProcessor<Map<String,Object>,InstrumentAttribute> {
    @Autowired
    com.fyntrac.common.entity.factory.InstrumentAttributeFactory instrumentAttributeFactory;
    @Override
    public InstrumentAttribute process(Map<String, Object> item) throws Exception {
        final InstrumentAttribute instrumentAttribute = new InstrumentAttribute();
        final Map<String, Object> attributes = new HashMap<>();
        Date effectiveDate = null;
        String instrumentId = "";
        String attributeId = "";

        for (Map.Entry<String, Object> entry : item.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if(key.equalsIgnoreCase("ACTIVITYUPLOADID")){
                continue;
            } else if (key.equalsIgnoreCase("EFFECTIVEDATE")) {
                effectiveDate = DateUtil.parseDate((String) value);
            } else if (key.equalsIgnoreCase("INSTRUMENTID")) {
                instrumentId = (String) value;
            } else if (key.equalsIgnoreCase("ATTRIBUTEID")) {
                attributeId = (String) value;
            } else {
                attributes.put(key, value);
            }
        }
        return instrumentAttributeFactory.create(instrumentId, attributeId, effectiveDate,0,attributes);
    }
}
