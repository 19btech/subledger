package com.fyntrac.common.entity.factory;

import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.enums.SequenceNames;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.SequenceGeneratorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Map;

@Component
public class InstrumentAttributeFactory {
    private final DataService dataService;

    @Autowired
    public InstrumentAttributeFactory(DataService dataService) {
        this.dataService = dataService;
    }
    private final String SEQUENCE="instrumentAttributeVersionId";
    // This method is thread-safe as it does not modify shared state
    public InstrumentAttribute create(String instrumentId,
                                      String attributeId,
                                      Date effectiveDate,
                                      int periodId,
                                      Map<String, Object> attributes) {
        long versionId = dataService.generateSequence(SequenceNames.INSTRUMENTATTRIBUTEVERSIONID.name());

        // Use the Builder pattern to create an instance
        return InstrumentAttribute.builder()
                .instrumentId(instrumentId)
                .attributeId(attributeId)
                .effectiveDate(effectiveDate)
                .periodId(periodId)
                .attributes(attributes)
                .versionId(versionId)
                .endDate(null) // Assuming endDate is initialized to null
                .build();
    }
}