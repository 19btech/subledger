package com.fyntrac.common.service;

import com.fyntrac.common.entity.InstrumentActivityReplayState;
import com.fyntrac.common.entity.InstrumentActivityState;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.repository.MemcachedRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
@Service
@Slf4j
public class InstrumentActivityReplayStateService extends CacheBasedService<InstrumentActivityReplayState> {

    @Autowired
    public InstrumentActivityReplayStateService(DataService<InstrumentActivityReplayState> dataService
            , MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);

    }

    @Override
    public void loadIntoCache() throws ExecutionException, InterruptedException {

    }

    @Override
    public InstrumentActivityReplayState save(InstrumentActivityReplayState activity) {
        return null;
    }

    @Override
    public Collection<InstrumentActivityReplayState> fetchAll() {
        return List.of();
    }

    public InstrumentActivityReplayState getReplayState(String instrumentId, String attributeId){
        Query query = new Query(Criteria.where("instrumentId").is(instrumentId).and("attributeId").is(attributeId));
        return  this.dataService.getMongoTemplate().findOne(query, InstrumentActivityReplayState.class);
    }
}
