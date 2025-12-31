package com.fyntrac.common.service;

import com.fyntrac.common.entity.InstrumentReplayState;
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
import java.util.Set;
import java.util.concurrent.ExecutionException;
@Service
@Slf4j
public class InstrumentReplayStateService extends CacheBasedService<InstrumentReplayState> {

    @Autowired
    public InstrumentReplayStateService(DataService<InstrumentReplayState> dataService
            , MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);

    }

    @Override
    public void loadIntoCache() throws ExecutionException, InterruptedException {

    }

    @Override
    public InstrumentReplayState save(InstrumentReplayState instrumentReplayState) {
        return this.dataService.save(instrumentReplayState);
    }

    public Collection<InstrumentReplayState> save(Set<InstrumentReplayState> instrumentReplayStates) {
        return this.dataService.saveAll(instrumentReplayStates, InstrumentReplayState.class);
    }
    @Override
    public Collection<InstrumentReplayState> fetchAll() {
        return List.of();
    }

    public InstrumentReplayState getInstrumentAttributeReplayState(String instrumentId, String attributeId, int postingDate) {
        Query query =
                new Query(Criteria.where("instrumentId").is(instrumentId).and("attributeId").is(attributeId).and(
                        "maxPostingDate").is(postingDate));
        return (InstrumentReplayState)this.dataService.getMongoTemplate().findOne(query, InstrumentReplayState.class);
    }

    public InstrumentReplayState getInstrumentReplayState(String instrumentId, int postingDate) {
        // Create an aggregation pipeline
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("instrumentId").is(instrumentId).and("maxPostingDate").is(postingDate)),
                Aggregation.group("instrumentId").min("minEffectiveDate").as("minEffectiveDate")
        );
        // Execute the aggregation
        AggregationResults<InstrumentReplayState> results =
                this.dataService.getMongoTemplate().aggregate(aggregation, InstrumentReplayState.class, InstrumentReplayState.class);
        // Get the result
        return results.getUniqueMappedResult();

    }
}
