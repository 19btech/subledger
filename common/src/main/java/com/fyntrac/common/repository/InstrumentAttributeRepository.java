package com.fyntrac.common.repository;

import com.fyntrac.common.entity.InstrumentAttribute;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface InstrumentAttributeRepository extends MongoRepository<InstrumentAttribute, String> {

    // Custom query to find InstrumentAttributes by attributeId, instrumentId, and where endDate is null
    @Query("{ 'attributeId': ?0, 'instrumentId': ?1, 'endDate': null }")
    List<InstrumentAttribute> findByAttributeIdAndInstrumentIdAndEndDateIsNull(String attributeId, String instrumentId);
    @Query("{ 'attributeId': ?0, 'instrumentId': ?1 }")
    List<InstrumentAttribute> findByAttributeIdAndInstrumentId(String attributeId, String instrumentId);

}