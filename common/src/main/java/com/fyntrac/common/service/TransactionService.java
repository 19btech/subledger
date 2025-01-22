package com.fyntrac.common.service;

import com.fyntrac.common.entity.Transactions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import com.fyntrac.common.dto.record.Records;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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
