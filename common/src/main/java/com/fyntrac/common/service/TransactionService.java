package com.fyntrac.common.service;

import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import com.fyntrac.common.dto.record.Records;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TransactionService extends CacheBasedService<Transactions>{


    @Autowired
    public TransactionService(DataService<Transactions> dataService, MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
    }

    @Override
    public void loadIntoCache() throws ExecutionException, InterruptedException {
        Collection<Transactions> transactions = this.dataService.fetchAllData(Transactions.class);
        CacheMap<Transactions> transactionsCacheMap = new CacheMap<>();
        for(Transactions transaction : transactions) {
            transactionsCacheMap.put(transaction.getName().toUpperCase(), transaction);
        }
        this.memcachedRepository.putInCache(Key.transactionsKey(this.dataService.getTenantId()), transactionsCacheMap);
    }

    public Transactions getTransaction(String transactionName) {
        String key = Key.transactionsKey(this.dataService.getTenantId());
        CacheMap<Transactions> map = this.memcachedRepository.getFromCache(key, CacheMap.class);
        Criteria.where("name").is(transactionName.toUpperCase());
        Query query = new Query(Criteria.where("name").is(transactionName.toUpperCase()));
        Transactions transaction = null;
        if(map != null) {
            transaction = map.getValue(transactionName.toUpperCase());
            if (transaction == null) {
                transaction = this.dataService.findOne(query, Transactions.class);
            }
        }else {
            map = new CacheMap<>();
            transaction = this.dataService.findOne(query, Transactions.class);
            if(transaction == null) {
                return null;
            }
            map.put(transaction.getName().toUpperCase(), transaction);
            this.memcachedRepository.putInCache(key, map);
        }
        return transaction;
    }
    public List<Transactions> getTransactionNames() {
        Query query = new Query();
        query.fields().include("name"); // Include only the 'name' field
        return  dataService.fetchData(query, Transactions.class);
    }

    public Collection<Transactions> getAll() {
        return dataService.fetchAllData(Transactions.class);
    }

    @Override
    public Transactions save(Transactions t) {
        return (Transactions) dataService.save(t);
    }

    @Override
    public Collection<Transactions> fetchAll() {
        return List.of();
    }


    public Collection<Records.TransactionNameRecord> fetchTransactinNames() {
        Collection<String> metrics = this.dataService.getMongoTemplate().query(Transactions.class)  // Replace Metric.class with your actual class
                .distinct("name")          // Specify the field name
                .as(String.class)                // Specify the return type
                .all();

        // Map the distinct names to MetricRecord objects
        return metrics.stream()
                .map(Records.TransactionNameRecord::new)         // Create a new MetricRecord for each distinct name
                .collect(Collectors.toList());
    }
}
