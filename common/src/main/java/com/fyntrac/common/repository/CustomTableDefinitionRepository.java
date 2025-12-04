package com.fyntrac.common.repository;

import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.enums.CustomTableType;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
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
}