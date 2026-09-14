package com.fyntrac.common.service;

import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.entity.Option;
import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.repository.TransactionsRepository;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import com.fyntrac.common.dto.record.Records;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TransactionService extends CacheBasedService<Transactions>{

    private final TransactionsRepository transactionsRepository;

    @Autowired
    public TransactionService(DataService<Transactions> dataService, MemcachedRepository memcachedRepository,
                              TransactionsRepository transactionsRepository) {
        super(dataService, memcachedRepository);
        this.transactionsRepository = transactionsRepository;
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
        Transactions transaction = null;
        if(map != null) {
            transaction = map.getValue(transactionName.toUpperCase());
            if (transaction == null) {
                transaction = this.getActiveTransactionByName(transactionName);
            }
        }else {
            map = new CacheMap<>();
            transaction = this.getActiveTransactionByName(transactionName);
            if(transaction == null) {
                return null;
            }
            map.put(transaction.getName().toUpperCase(), transaction);
            this.memcachedRepository.putInCache(key, map);
        }
        return transaction;
    }
    public List<Transactions> getTransactionNames() {
        return transactionsRepository.findActiveTransactionNamesOnly();
    }

    public Collection<Transactions> getAll() {
        return this.transactionsRepository.findByIsDeletedFalse();
    }

    @Override
    public Transactions save(Transactions t) {
        if (t == null || t.getName() == null || t.getName().isBlank()) {
            throw new IllegalArgumentException("Cannot save a transaction configuration without a valid name mapping.");
        }

        // 1. Look for an existing transaction with the same name across all global states
        Optional<Transactions> existingTransactionOpt = this.transactionsRepository.findByNameIgnoreCase(t.getName().trim());

        if (existingTransactionOpt.isPresent()) {
            Transactions existingTransaction = existingTransactionOpt.get();

            // 2. Map the ID of the existing document onto the incoming object to force an update instead of an insert
            t.setId(existingTransaction.getId());

            // 3. Reactivate the record if it was previously soft-deleted
            t.setDeleted(Boolean.FALSE);

            log.info("Existing transaction blueprint recognized for name [{}]. Merging metadata and setting isDeleted to FALSE.", t.getName());
        } else {
            // 4. Ensure new documents baseline with an active operational status explicitly
            t.setDeleted(Boolean.FALSE);
            log.debug("No pre-existing record found for name [{}]. Initializing new transaction state model.", t.getName());
        }

        // 5. Execute the update/insert atomically via your core persistence interface layer
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

    public Collection<Option> fetchTransactionOptions() {
        Collection<String> metrics = this.dataService.getMongoTemplate().query(Transactions.class)  // Replace Metric.class with your actual class
                .distinct("name")          // Specify the field name
                .as(String.class)                // Specify the return type
                .all();

        // Map the distinct names to MetricRecord objects
        return metrics.stream()
                .map(s -> Option.builder().label(s).value(s).build())  // Lambda: each string becomes label
        // and value
                .collect(Collectors.toList());  // Collect into a List (or use toCollection for specific type)
    }

    /**
     * Checks if a transaction exists by name across the entire history,
     * regardless of whether it is soft-deleted or active.
     */
    public boolean checkGlobalExistenceByName(String name) {
        if (name == null || name.isBlank()) return false;

        log.debug("Checking global database existence for transaction name: [{}]", name);
        return transactionsRepository.existsByNameIgnoreCase(name.trim());
    }

    /**
     * Checks if an ACTIVE (non-deleted) transaction configuration exists by its name.
     * Useful for pre-upload validation rules.
     */
    public boolean isTransactionActiveAndExists(String name) {
        if (name == null || name.isBlank()) return false;

        log.debug("Verifying system presence for live transaction profile name: [{}]", name);
        return transactionsRepository.existsByNameIgnoreCaseAndIsDeletedFalse(name.trim());
    }

    /**
     * Fetches a single active transaction configuration document by its name.
     * Throws an exception if the transaction does not exist or has been soft-deleted.
     *
     * @throws NoSuchElementException if the record isn't found or is marked as deleted.
     */
    public Transactions getActiveTransactionByName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Transaction name lookups require a non-blank token string.");
        }

        log.debug("Fetching single active transaction entity for identifier: [{}]", name);
        return transactionsRepository.findByNameIgnoreCaseAndIsDeletedFalse(name.trim())
                .orElseThrow(() -> new NoSuchElementException(
                        "No active transaction configuration found matching the name: " + name));
    }

    /**
     * Retrieves all active (non-deleted) transactions configured in the system.
     * Primary use case includes caching layer lookups or validation mapping matrices.
     *
     * @return List of active transactions, or an empty list if none are found.
     */
    public List<Transactions> getAllActiveTransactions() {
        log.debug("Fetching collection list of all non-deleted transactions.");
        List<Transactions> activeTransactions = transactionsRepository.findByIsDeletedFalse();

        if (activeTransactions == null) {
            return Collections.emptyList();
        }

        log.info("Successfully retrieved {} active transaction records from collection.", activeTransactions.size());
        return activeTransactions;
    }

    /**
     * Soft-deletes a transaction blueprint using its unique identity sequence string.
     *
     * @param id The database primary key token.
     */
    public void removeTransactionById(String id) {
        log.warn("Initiating soft-delete sequence for transaction record ID: [{}]", id);
        long modifiedCount = transactionsRepository.softDeleteById(id);

        if (modifiedCount == 0) {
            log.warn("Soft-delete executed but zero records were updated for ID: [{}]", id);
        } else {
            log.info("Successfully flagged transaction ID [{}] as isDeleted=true.", id);
            try {
                this.memcachedRepository.delete(Key.transactionsKey(this.dataService.getTenantId()));
            } catch (Exception e) {
                log.error("Failed to evict transaction cache for tenant: {}", this.dataService.getTenantId(), e);
            }
        }
    }

    /**
     * Soft-deletes an active transaction using its case-insensitive name profile.
     *
     * @param name The system transaction lookup name.
     */
    public void removeTransactionByName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Cannot execute soft-delete on an empty transaction name string.");
        }

        String cleanedName = name.trim();
        log.warn("Initiating soft-delete verification for transaction blueprint named: [{}]", cleanedName);

        Transactions t =  getTransaction(cleanedName);
       t.setDeleted(Boolean.TRUE);
       Transactions deletedTransaction = this.transactionsRepository.save(t);
        if (!deletedTransaction.isDeleted()) {
            throw new java.util.NoSuchElementException(
                    "No active, non-deleted transaction layout was found matching the name: " + cleanedName
            );
        }

        log.info("Successfully soft-deleted transaction template [{}]. Updated records count: {}", cleanedName, deletedTransaction.toString());
        try {
            this.memcachedRepository.delete(Key.transactionsKey(this.dataService.getTenantId()));
        } catch (Exception e) {
            log.error("Failed to evict transaction cache for tenant: {}", this.dataService.getTenantId(), e);
        }
    }
}
