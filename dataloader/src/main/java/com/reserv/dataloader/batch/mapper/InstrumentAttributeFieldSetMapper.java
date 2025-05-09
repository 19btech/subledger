package com.reserv.dataloader.batch.mapper;

import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import java.util.HashMap;

public class InstrumentAttributeFieldSetMapper implements FieldSetMapper<InstrumentAttribute> {

    @Override
    public InstrumentAttribute mapFieldSet(FieldSet fieldSet) throws BindException {
        InstrumentAttribute instrumentAttribute = InstrumentAttribute.builder()
                .effectiveDate(fieldSet.readDate("effectiveDate"))
                .instrumentId(fieldSet.readString("instrumentId"))
                .attributeId(fieldSet.readString("attributeId"))
                .postingDate(DateUtil.dateInNumber(fieldSet.readDate("postingDate")))
                .attributes(new HashMap<>())
                .build();

        for (int i = 5; i < fieldSet.getValues().length; i++) {
            String key = fieldSet.getNames()[i];
            Object value = fieldSet.getValues()[i];
            instrumentAttribute.getAttributes().put(key, value);
        }

        return instrumentAttribute;
    }
}