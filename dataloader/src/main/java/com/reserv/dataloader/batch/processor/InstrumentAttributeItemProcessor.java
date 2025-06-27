package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
        int postingDate=0;


        for (Map.Entry<String, Object> entry : item.entrySet()) {
            String key = entry.getKey();
            if(key.isBlank()){
                continue;
            }
            Object value = entry.getValue();
            if(key.equalsIgnoreCase("ACTIVITYUPLOADID")){
                continue;
            } else if (key.equalsIgnoreCase("EFFECTIVEDATE")) {
                LocalDate localDate = LocalDate.parse((String) value, DateTimeFormatter.ofPattern("M/dd/yyyy"));
                effectiveDate = Date.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());
            } else if (key.equalsIgnoreCase("INSTRUMENTID")) {
                instrumentId = (String) value;
            } else if (key.equalsIgnoreCase("ATTRIBUTEID")) {
                attributeId = (String) value;
            } else if (key.equalsIgnoreCase("POSTINGDATE")) {
                Date pDate = DateUtil.parseDate((String) value);
                postingDate = DateUtil.dateInNumber(pDate);
        }else {
                if(key.isBlank()){
                    continue;
                }
                attributes.put(key, value);
            }
        }
        return instrumentAttributeFactory.create(instrumentId, attributeId, effectiveDate,0, postingDate, Source.ETL,attributes);
    }
}
