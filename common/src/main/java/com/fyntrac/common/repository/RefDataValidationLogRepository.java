package com.fyntrac.common.repository;

import com.fyntrac.common.entity.RefDataValidationLog;
import com.fyntrac.common.enums.ValidationType;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface RefDataValidationLogRepository extends MongoRepository<RefDataValidationLog, String> {

    List<RefDataValidationLog> findByJobId(Long jobId);

    List<RefDataValidationLog> findByValidationType(ValidationType validationType);

    List<RefDataValidationLog> findByJobIdAndValidationType(Long jobId, ValidationType validationType);

    List<RefDataValidationLog> findBySourceTable(String sourceTable);

    List<RefDataValidationLog> findByJobIdAndSourceTable(Long jobId, String sourceTable);

    List<RefDataValidationLog> findByErrorCode(String errorCode);

    List<RefDataValidationLog> findByJobIdAndErrorCode(Long jobId, String errorCode);
}
