package com.fyntrac.common.service;

import com.fyntrac.common.entity.Sequence;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SequenceGenerator {
    public long generateInstrumentAttributeVersionId(MongoTemplate mongoTemplate) {
        String sequenceName = "instrumentAttributeVersionId";

        // Check if the sequence exists
        Query query = new Query(Criteria.where("id").is(sequenceName));
        Sequence sequence = mongoTemplate.findOne(query, Sequence.class);

        if (sequence == null) {
            // If the sequence does not exist, create it with an initial value of 0
            sequence = new Sequence();
            sequence.setId(sequenceName);
            sequence.setSeq(0);
            mongoTemplate.insert(sequence);
        }

        // Increment the sequence and return the new value
        Update update = new Update().inc("seq", 1);
        sequence = mongoTemplate.findAndModify(query, update, FindAndModifyOptions.options().returnNew(true), Sequence.class);

        return sequence.getSeq();
    }

    public void generateAllSequences(MongoTemplate mongoTemplate, String tenant) {
        if(mongoTemplate == null){
            log.error("MongoTemplate is null for tenant[{}]", tenant);
        }
            this.generateInstrumentAttributeVersionId(mongoTemplate);
    }
}