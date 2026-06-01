package com.fyntrac.common.repository;

import com.fyntrac.common.entity.GeneralLedgerEntery;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface GeneralLedgerEnteryRepository extends MongoRepository<GeneralLedgerEntery, String> {

    // Delete all GeneralLedgerEntery records for a given posting date
    void deleteByPostingDate(Integer postingDate);
}

