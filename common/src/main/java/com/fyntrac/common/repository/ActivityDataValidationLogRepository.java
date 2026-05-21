package com.fyntrac.common.repository;

import com.fyntrac.common.entity.ActivityDataValidationLog;
import com.fyntrac.common.enums.ValidationType;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ActivityDataValidationLogRepository
        extends MongoRepository<ActivityDataValidationLog, String> {

    List<ActivityDataValidationLog> findByJobId(Long jobId);

    List<ActivityDataValidationLog> findByValidationType(ValidationType validationType);

    List<ActivityDataValidationLog> findByJobIdAndValidationType(Long jobId, ValidationType validationType);

    List<ActivityDataValidationLog> findByErrorCode(String errorCode);

    List<ActivityDataValidationLog> findByJobIdAndErrorCode(Long jobId, String errorCode);

    List<ActivityDataValidationLog> findByInstrumentId(String instrumentId);

    List<ActivityDataValidationLog> findByJobIdAndInstrumentId(Long jobId, String instrumentId);
}
