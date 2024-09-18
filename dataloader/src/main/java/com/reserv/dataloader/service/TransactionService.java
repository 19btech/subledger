package com.reserv.dataloader.service;

import com.reserv.dataloader.entity.Transactions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;

@Service
@Slf4j
public class TransactionService {

    private final DataService dataService;

    @Autowired
    public TransactionService(DataService dataService) {
        this.dataService = dataService;
    }

    public List<Transactions> getTransactionNames() {
        Query query = new Query();
        query.fields().include("name"); // Include only the 'name' field
        return  dataService.fetchData(query, Transactions.class);
    }

    public Collection<Transactions> getAll() {
        return dataService.fetchAllData(Transactions.class);
    }

    public Transactions save(Transactions t) {
        return (Transactions) dataService.save(t);
    }
}
