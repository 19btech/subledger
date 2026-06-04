package com.fyntrac.common.repository;

import com.fyntrac.common.entity.ExecutionInstance;
import com.fyntrac.common.entity.ExecutionState;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Repository
public interface ExecutionStateRepository extends MongoRepository<ExecutionState, String> {

    // 1. Returns a single object by unique execution date integers
    Optional<ExecutionState> findByExecutionDate(Integer executionDate);

    //2.
    Optional<ExecutionState> findByLastExecutionDate(Integer lastExecutionDate);


    // 3. Custom JSON Query returning a single object directly
    @Query("{ 'executionDate' : ?0 }")
    Optional<ExecutionState> findCustomByExecutionDate(Integer executionDate);

    @Transactional
    void deleteByExecutionDate(Integer postingDate);

    @Transactional
    void deleteByExecutionDateGreaterThanEqual(Integer postingDate);

    @Transactional
    void deleteByExecutionDateGreaterThan(Integer postingDate);
}
