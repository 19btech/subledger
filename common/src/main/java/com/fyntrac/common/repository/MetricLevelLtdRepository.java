package com.fyntrac.common.repository;

import com.fyntrac.common.entity.MetricLevelLtd;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MetricLevelLtdRepository extends MongoRepository<MetricLevelLtd, String> {

    // Delete all MetricLevelLtd records for a given posting date
    void deleteByPostingDate(Integer postingDate);
}
