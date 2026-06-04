package com.fyntrac.common.repository;

import com.fyntrac.common.entity.InstrumentLevelLtd;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface InstrumentLevelLtdRepository extends MongoRepository<InstrumentLevelLtd, String> {

    // Delete all InstrumentLevelLtd records for a given posting date
    @Transactional
    void deleteByPostingDate(Integer postingDate);

    @Transactional
    void deleteByPostingDateGreaterThanEqual(Integer postingDate);
}
