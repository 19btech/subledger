package com.fyntrac.common.repository;

import com.fyntrac.common.entity.ActivityLog;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ActivityLogRepo extends MongoRepository<ActivityLog, String> {
}
