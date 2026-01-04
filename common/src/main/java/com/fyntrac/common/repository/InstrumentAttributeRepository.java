package com.fyntrac.common.repository;

import com.fyntrac.common.entity.InstrumentAttribute;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public interface InstrumentAttributeRepository extends MongoRepository<InstrumentAttribute, String> {

    // Custom query to find InstrumentAttributes by attributeId, instrumentId, and where endDate is null
    @Query("{ 'attributeId': ?0, 'instrumentId': ?1, 'endDate': null }")
    List<InstrumentAttribute> findByAttributeIdAndInstrumentIdAndEndDateIsNull(String attributeId, String instrumentId);
    @Query("{ 'attributeId': ?0, 'instrumentId': ?1 }")
    List<InstrumentAttribute> findByAttributeIdAndInstrumentId(String attributeId, String instrumentId);
    @Query("{ 'attributeId': ?0, 'instrumentId': ?1, 'previousVersionId': 0 }")
    List<InstrumentAttribute> findByAttributeIdAndInstrumentIdAndPreviousVersionIdIsZero(String attributeId, String instrumentId);

    // Custom query to find InstrumentAttributes by attributeId, instrumentId, and where endDate is null
    @Query("{ 'postingDate': ?0, 'endDate': null }")
    List<InstrumentAttribute> findAllByPostingDate(Integer postingDate);

    @Query("{ 'instrumentId': ?0, 'attributeId': ?1, 'postingDate': ?2, 'endDate': null }")
    InstrumentAttribute findByInstrumentIdAndPostingDate(String instrumentId,String attributeId, Integer postingDate);

    @Query("{ 'instrumentId': ?0, 'attributeId': ?1, 'effectiveDate': { $gte: ?2 }, 'endDate': null }")
    List<InstrumentAttribute> findByInstrumentIdAndAttributeIdAndEffectiveDateGte(String instrumentId, String attributeId, Date effectiveDate);

    @Query("{ 'versionId': ?0 }")
    InstrumentAttribute findByVersionId(Long versionId);

    @Query("{ 'endDate': null }")
    List<InstrumentAttribute> findAll();

    // --- NEW: Pagination Support ---
    Page<InstrumentAttribute> findAllByEndDateIsNull(Pageable pageable);

}