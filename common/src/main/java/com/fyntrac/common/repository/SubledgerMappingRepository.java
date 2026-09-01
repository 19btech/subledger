package com.fyntrac.common.repository;

import com.fyntrac.common.entity.SubledgerMapping;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SubledgerMappingRepository extends MongoRepository<SubledgerMapping, String> {
    void deleteById(String id);
    void delete(SubledgerMapping entity);
    void deleteAll();
}
