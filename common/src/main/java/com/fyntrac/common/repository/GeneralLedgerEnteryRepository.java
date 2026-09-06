package com.fyntrac.common.repository;

import com.fyntrac.common.entity.GeneralLedgerEntery;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface GeneralLedgerEnteryRepository extends MongoRepository<GeneralLedgerEntery, String> {

    // Delete all GeneralLedgerEntery records for a given posting date
    @Transactional
    long deleteByPostingDate(Integer postingDate);

    @Transactional
    void deleteByPostingDateGreaterThanEqual(Integer postingDate);
}

