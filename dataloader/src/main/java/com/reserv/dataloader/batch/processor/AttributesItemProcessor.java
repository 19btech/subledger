package com.reserv.dataloader.batch.processor;

import com.reserv.dataloader.entity.Attributes;
import com.reserv.dataloader.entity.Transactions;
import org.springframework.batch.item.ItemProcessor;

public class AttributesItemProcessor implements ItemProcessor<Attributes,Attributes> {
    @Override
    public Attributes process(Attributes item) throws Exception {
        final Attributes attribute = new Attributes();
        attribute.setIsNullable(item.getIsNullable());
        attribute.setAttributeName(item.getAttributeName());
        attribute.setIsReclassable(item.getIsReclassable());
        attribute.setDataType(item.getDataType());
        attribute.setUserField(item.getUserField());
        return attribute;
    }

}