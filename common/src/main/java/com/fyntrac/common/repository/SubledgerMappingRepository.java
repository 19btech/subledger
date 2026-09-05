package com.fyntrac.common.repository;

import com.fyntrac.common.entity.SubledgerMapping;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SubledgerMappingRepository extends MongoRepository<SubledgerMapping, String> {

    /**
     * Fetches all active (non soft-deleted) subledger mappings. Records missing the isDeleted
     * field entirely (pre-existing data) are treated as active.
     */
    @Query("{ '$or': [ { 'isDeleted': false }, { 'isDeleted': { '$exists': false } } ] }")
    List<SubledgerMapping> findByIsDeletedFalse();

    /**
     * Soft-deletes a single subledger mapping document by its unique database ID.
     *
     * @return the number of modified documents (1 if found).
     */
    @Query("{ '_id' : ?0 }")
    @Update("{ '$set' : { 'isDeleted' : true } }")
    long softDeleteById(String id);

    void deleteById(String id);
    void delete(SubledgerMapping entity);
    void deleteAll();
}
