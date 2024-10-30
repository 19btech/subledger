package com.fyntrac.common.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.data.mongodb.core.MongoTemplate;

@Service
public class SequenceGeneratorService {

    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    SequenceGenerator sequenceGenerator;

    public long generateInstrumentAttributeVersionId() {
        return this.sequenceGenerator.generateInstrumentAttributeVersionId(this.mongoTemplate);
    }
}