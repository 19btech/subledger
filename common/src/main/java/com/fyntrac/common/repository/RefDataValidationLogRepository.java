package com.fyntrac.common.repository;

import com.fyntrac.common.entity.RefDataValidationLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RefDataValidationLogRepository extends JpaRepository<RefDataValidationLog, Long> {
}
