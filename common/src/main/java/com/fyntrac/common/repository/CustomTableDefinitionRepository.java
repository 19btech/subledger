package com.fyntrac.common.repository;

import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.enums.CustomTableType;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CustomTableDefinitionRepository extends MongoRepository<CustomTableDefinition, String> {

    Optional<CustomTableDefinition> findByTableName(String tableName);

    boolean existsByTableName(String tableName);

    Optional<List<CustomTableDefinition>> findByTableType(CustomTableType tableType);

    @Query("{ 'columns.columnName': ?0 }")
    List<CustomTableDefinition> findByColumnName(String columnName);

    @Query("{ 'tableName': { $regex: ?0, $options: 'i' } }")
    List<CustomTableDefinition> findByTableNameContainingIgnoreCase(String tableName);

    @Query("{ 'description': { $regex: ?0, $options: 'i' } }")
    List<CustomTableDefinition> findByDescriptionContainingIgnoreCase(String description);

    @Query("{ 'tableType': ?0 }")
    List<CustomTableDefinition> findByTableTypeIgnoreCase(CustomTableType tableType);

    List<CustomTableDefinition> findAllBy();

    /**
     * Fetches all active (non soft-deleted) table definitions of the given type. Records missing
     * the isDeleted field entirely (pre-existing data) are treated as active.
     */
    @Query("{ 'tableType': ?0, '$or': [ { 'isDeleted': false }, { 'isDeleted': { '$exists': false } } ] }")
    List<CustomTableDefinition> findByTableTypeAndIsDeletedFalse(CustomTableType tableType);

    /**
     * Fetches every still-active table of the given type whose referenceTable points at the
     * given name - i.e. the OPERATIONAL tables that would be orphaned if that REFERENCE table
     * were removed. Used to cascade soft-deletes across a reference/operational pair.
     */
    @Query("{ 'tableType': ?0, 'referenceTable': ?1, '$or': [ { 'isDeleted': false }, { 'isDeleted': { '$exists': false } } ] }")
    List<CustomTableDefinition> findByTableTypeAndReferenceTableAndIsDeletedFalse(CustomTableType tableType, String referenceTable);

    /**
     * Soft-deletes a single table definition document by its unique database ID. Only flags the
     * definition - never touches the underlying physical Mongo collection or its data.
     *
     * @return the number of modified documents (1 if found).
     */
    @Query("{ '_id' : ?0 }")
    @Update("{ '$set' : { 'isDeleted' : true } }")
    long softDeleteById(String id);
}