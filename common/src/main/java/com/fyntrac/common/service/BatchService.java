package com.fyntrac.common.service;

import com.fyntrac.common.entity.Batch;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.enums.BatchStatus;
import com.fyntrac.common.enums.BatchType;
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
public class BatchService extends  CacheBasedService<Batch>{

    @Autowired
    public BatchService(DataService<Batch> dataService
            , MemcachedRepository memcachedRepository){
        super(dataService, memcachedRepository);
    }

    @Override
    public void loadIntoCache() throws ExecutionException, InterruptedException {

    }

    @Override
    public Batch save(Batch batch) {
        return this.dataService.save(batch);
    }

    @Override
    public Collection<Batch> fetchAll() {
        return  this.dataService.fetchAllData(Batch.class);
    }

    public Collection<Batch> getBatchesByStatus(BatchStatus status) {
        Query query = new Query(Criteria.where("batchStatus").is(status.name()));
        return this.dataService.fetchData(query, Batch.class);
    }

    public Collection<Batch> getBatchesByTypeAndStatus(BatchType type, BatchStatus status) {
        Query query = new Query(Criteria.where("batchStatus").is(status.name()).and("batchType").is(type.name()));
        return this.dataService.fetchData(query, Batch.class);
    }
}
