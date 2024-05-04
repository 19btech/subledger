package com.reserv.dataloader.repository;

import com.reserv.dataloader.entity.Transactions;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface TransactionsRepo extends MongoRepository<Transactions, String> {
}
