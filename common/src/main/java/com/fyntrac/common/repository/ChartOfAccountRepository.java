package com.fyntrac.common.repository;

import com.fyntrac.common.entity.ChartOfAccount;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ChartOfAccountRepository extends MongoRepository<ChartOfAccount, String> {
    void deleteById(String id);
    void delete(ChartOfAccount entity);
    void deleteAll();
}
