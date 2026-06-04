package com.fyntrac.common.repository;

import com.fyntrac.common.entity.GeneralLedgerEnteryStage;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface GeneralLedgerEnteryStageRepository extends MongoRepository<GeneralLedgerEnteryStage, String> {

    // Delete all GeneralLedgerEnteryStage records for a given posting date
    @Transactional
    void deleteByPostingDate(Integer postingDate);

    @Transactional
    void deleteByPostingDateGreaterThanEqual(Integer postingDate);
}
