package com.fyntrac.common.repository;

import com.fyntrac.common.entity.ExecutionInstance;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ExecutionInstanceRepository extends MongoRepository<ExecutionInstance, String> {
    Optional<ExecutionInstance> findById(String id);
}
