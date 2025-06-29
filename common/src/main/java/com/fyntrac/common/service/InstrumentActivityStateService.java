package com.fyntrac.common.service;

import com.fyntrac.common.entity.InstrumentActivityState;
import com.fyntrac.common.repository.MemcachedRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class InstrumentActivityStateService extends CacheBasedService<InstrumentActivityState> {

    @Autowired
    public InstrumentActivityStateService(DataService<InstrumentActivityState> dataService
            , MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);

    }

    @Override
    public void loadIntoCache() throws ExecutionException, InterruptedException {

    }

    @Override
    public InstrumentActivityState save(InstrumentActivityState instrumentActivityState) {
        return null;
    }

    @Override
    public Collection<InstrumentActivityState> fetchAll() {
        return List.of();
    }

    public InstrumentActivityState getActivityState(String instrumentId, String attributeId){
        Query query = new Query(Criteria.where("instrumentId").is(instrumentId).and("attributeId").is(attributeId));
        return  this.dataService.getMongoTemplate().findOne(query, InstrumentActivityState.class);
    }

    public InstrumentActivityState getActivityState(String instrumentId) {
        // Create an aggregation pipeline
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("instrumentId").is(instrumentId)),
                Aggregation.group("instrumentId").min("maxTransactionDate").as("maxTransactionDate")
        );
        // Execute the aggregation
        AggregationResults<InstrumentActivityState> results =
                this.dataService.getMongoTemplate().aggregate(aggregation, InstrumentActivityState.class, InstrumentActivityState.class);
        // Get the result
        return results.getUniqueMappedResult();

    }
}
