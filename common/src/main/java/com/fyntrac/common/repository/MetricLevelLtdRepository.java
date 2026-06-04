package com.fyntrac.common.repository;

import com.fyntrac.common.entity.MetricLevelLtd;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface MetricLevelLtdRepository extends MongoRepository<MetricLevelLtd, String> {

    // Delete all MetricLevelLtd records for a given posting date
    @Transactional
    void deleteByPostingDate(Integer postingDate);

    @Transactional
    void deleteByPostingDateGreaterThanEqual(Integer postingDate);
}
