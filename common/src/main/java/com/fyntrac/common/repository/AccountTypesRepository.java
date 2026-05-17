package com.fyntrac.common.repository;

import com.fyntrac.common.entity.AccountTypes;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AccountTypesRepository extends MongoRepository<AccountTypes, String> {
}
