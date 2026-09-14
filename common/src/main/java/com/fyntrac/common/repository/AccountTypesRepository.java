package com.fyntrac.common.repository;

import com.fyntrac.common.entity.AccountTypes;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AccountTypesRepository extends MongoRepository<AccountTypes, String> {

    /**
     * Fetches all active (non soft-deleted) account types. Records missing the isDeleted
     * field entirely (pre-existing data) are treated as active.
     */
    @Query("{ '$or': [ { 'isDeleted': false }, { 'isDeleted': { '$exists': false } } ] }")
    List<AccountTypes> findByIsDeletedFalse();

    /**
     * Soft-deletes a single account type document by its unique database ID.
     *
     * @return the number of modified documents (1 if found).
     */
    @Query("{ '_id' : ?0 }")
    @Update("{ '$set' : { 'isDeleted' : true } }")
    long softDeleteById(String id);

    void deleteById(String id);
    void delete(AccountTypes entity);
    void deleteAll();
}
