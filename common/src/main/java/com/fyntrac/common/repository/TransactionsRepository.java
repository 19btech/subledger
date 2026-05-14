package com.fyntrac.common.repository;

import com.fyntrac.common.entity.Transactions;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TransactionsRepository extends MongoRepository<Transactions, String> {
    boolean existsByNameIgnoreCase(String name);
}
