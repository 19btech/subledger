package com.fyntrac.common.repository;

import com.fyntrac.common.entity.InstrumentLevelLtd;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface InstrumentLevelLtdRepository extends MongoRepository<InstrumentLevelLtd, String> {

    // Delete all InstrumentLevelLtd records for a given posting date
    void deleteByPostingDate(Integer postingDate);
}
