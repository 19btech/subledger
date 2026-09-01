package com.fyntrac.common.repository;

import com.fyntrac.common.entity.Attributes;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

/**
 * Repository for Attributes entity, providing multi-tenant safe data access via Spring Data MongoDB.
 */
@Repository
public interface AttributesRepository extends MongoRepository<Attributes, String> {
    void deleteById(String id);
    void delete(Attributes entity);
    void deleteAll();
}
