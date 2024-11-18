package com.fyntrac.common.entity.factory;

import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.enums.DataType;
import com.fyntrac.common.service.SequenceGeneratorService;
import org.springframework.beans.factory.annotation.Autowired;

public class AttributeFactory {

    private final SequenceGeneratorService sequenceGeneratorService;

    @Autowired
    public AttributeFactory(SequenceGeneratorService sequenceGeneratorService) {
        this.sequenceGeneratorService = sequenceGeneratorService;
    }

    public Attributes create(String userField
                            ,String attributeName
                            , int isReclassable
                            , DataType dataType
                            , int isNullable) {
        long sequence = sequenceGeneratorService.generateAttributeSequence();
        return Attributes.builder()
                .userField(userField)
                .attributeName(attributeName)
                .isReclassable(isReclassable)
                .dataType(dataType)
                .isNullable(isNullable)
                .sequenceId(sequence).build();
    }
}
