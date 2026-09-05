package com.fyntrac.common.repository;

import com.fyntrac.common.entity.Attributes;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Repository for Attributes entity, providing multi-tenant safe data access via Spring Data MongoDB.
 */
@Repository
public interface AttributesRepository extends MongoRepository<Attributes, String> {

    /**
     * Fetches all active (non soft-deleted) attributes. Records missing the isDeleted
     * field entirely (pre-existing data) are treated as active.
     */
    @Query("{ '$or': [ { 'isDeleted': false }, { 'isDeleted': { '$exists': false } } ] }")
    List<Attributes> findByIsDeletedFalse();

    /**
     * Soft-deletes a single attribute document by its unique database ID.
     *
     * @return the number of modified documents (1 if found).
     */
    @Query("{ '_id' : ?0 }")
    @Update("{ '$set' : { 'isDeleted' : true } }")
    long softDeleteById(String id);

    void deleteById(String id);
    void delete(Attributes entity);
    void deleteAll();
}
