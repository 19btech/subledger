package com.fyntrac.common.repository;

import com.fyntrac.common.entity.Transactions;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TransactionsRepository extends MongoRepository<Transactions, String> {
    boolean existsByNameIgnoreCase(String name);

    Optional<Transactions> findByNameIgnoreCase(String name);

    /**
     * Fetches all transactions but only includes the 'name' field in the payload wrapper.
     * The empty brackets '{}' means match all documents in the collection.
     * The value '1' indicates an explicit include rule.
     */
    @Query(value = "{}", fields = "{ 'name' : 1 }")
    List<Transactions> findAllTransactionNamesOnly();

    /**
     * Hybrid variant: Fetches transaction names only for active records (isDeleted: false)
     */
    @Query(value = "{ 'isDeleted': false }", fields = "{ 'name' : 1 }")
    List<Transactions> findActiveTransactionNamesOnly();

    /**
     * Soft-deletes a single transaction document by its unique database ID.
     * * @param id The target document String identifier.
     * @return The number of modified documents (should be 1 if found).
     */
    @Query("{ '_id' : ?0 }")
    @Update("{ '$set' : { 'isDeleted' : true } }")
    long softDeleteById(String id);

    /**
     * Soft-deletes an active transaction blueprint by its unique system name configuration.
     *
     * @param name The transaction system identifier name token.
     * @return The number of documents updated.
     */
    @Query("{ 'name' : { '$regex': '^?0$', '$options': 'i' }, 'isDeleted' : false }")
    @Update("{ '$set' : { 'isDeleted' : true } }")
    long softDeleteByNameIgnoreCase(String name);

    // Checks if active: Matches if isDeleted is explicitly false OR if it doesn't exist yet
    @Query("{ 'name': { '$regex': '^?0$', '$options': 'i' }, '$or': [ { 'isDeleted': false }, { 'isDeleted': { '$exists': false } } ] }")
    boolean existsByNameIgnoreCaseAndIsDeletedFalse(String name);

    @Query("{ 'name': { '$regex': '^?0$', '$options': 'i' }, '$or': [ { 'isDeleted': false }, { 'isDeleted': { '$exists': false } } ] }")
    Optional<Transactions> findByNameIgnoreCaseAndIsDeletedFalse(String name);

    @Query("{ '$or': [ { 'isDeleted': false }, { 'isDeleted': { '$exists': false } } ] }")
    List<Transactions> findByIsDeletedFalse();

    void deleteById(String id);
    void delete(Transactions entity);
    void deleteAll();
}
