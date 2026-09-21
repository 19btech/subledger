package com.fyntrac.common.repository;

import com.fyntrac.common.entity.InstrumentAttribute;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Collection;
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

    @Query("{ 'instrumentId': ?0, 'attributeId': ?1, 'effectiveDate': { $gte: ?2 } }")
    List<InstrumentAttribute> findByInstrumentIdAndAttributeIdAndEffectiveDateGte(String instrumentId, String attributeId, Date effectiveDate);

    @Query("{ 'versionId': ?0 }")
    InstrumentAttribute findByVersionId(Long versionId);

    @Query("{ 'endDate': null }")
    List<InstrumentAttribute> findAll();

    // --- NEW: Pagination Support ---
    Page<InstrumentAttribute> findAllByEndDateIsNull(Pageable pageable);

    // --- Keyset (cursor) pagination over ACTIVE rows, ordered by instrumentId ---
    // skip/limit pagination over this collection is not safe for batching: an instrument
    // holds one active row per attributeId, so page boundaries split an instrument's rows
    // across two pages (and an unsorted skip/limit can return the same document twice).
    // These two methods let callers walk the collection by instrumentId instead, so a given
    // instrument lands in exactly one batch. Pass the page size via the Pageable; the caller
    // supplies the ascending instrumentId sort.
    @Query("{ 'endDate': null }")
    List<InstrumentAttribute> findActiveOrderedByInstrumentId(Pageable pageable);

    @Query("{ 'endDate': null, 'instrumentId': { $gt: ?0 } }")
    List<InstrumentAttribute> findActiveAfterInstrumentId(String instrumentId, Pageable pageable);

    // Full active row set for a single instrument — used to complete a group that would
    // otherwise straddle a page boundary.
    @Query("{ 'endDate': null, 'instrumentId': ?0 }")
    List<InstrumentAttribute> findActiveByInstrumentId(String instrumentId);

    // Existence checks for a batch of IDs against ACTIVE rows, projected down to the one field
    // asked about. Loader processors validate each chunk's IDs with these instead of preloading
    // the whole collection (findAll) into heap, which grew with the instrument count and OOM'd.
    // Only the projected field is populated on the returned entities.
    @Query(value = "{ 'instrumentId': { $in: ?0 }, 'endDate': null }", fields = "{ 'instrumentId': 1 }")
    List<InstrumentAttribute> findActiveInstrumentIdsIn(Collection<String> instrumentIds);

    @Query(value = "{ 'attributeId': { $in: ?0 }, 'endDate': null }", fields = "{ 'attributeId': 1 }")
    List<InstrumentAttribute> findActiveAttributeIdsIn(Collection<String> attributeIds);

    // Delete all InstrumentAttribute records for a given posting date
    long deleteByPostingDate(Integer postingDate);

    boolean existsByInstrumentId(String instrumentId);
    boolean existsByAttributeId(String attributeId);
}