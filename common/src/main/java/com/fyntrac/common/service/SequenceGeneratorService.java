package com.fyntrac.common.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.Serial;
import java.io.Serializable;

@Service
public class SequenceGeneratorService implements Serializable {

    @Serial
    private static final long serialVersionUID = 8411523347970701430L;

    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    SequenceGenerator sequenceGenerator;

    public long generateInstrumentAttributeVersionId() {
        return this.sequenceGenerator.generateInstrumentAttributeVersionId(this.mongoTemplate);
    }

    public long generateAttributeSequence(){
        return this.sequenceGenerator.generateAttributeSequence(this.mongoTemplate);
    }
}