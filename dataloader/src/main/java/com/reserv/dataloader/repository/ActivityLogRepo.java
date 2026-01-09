package com.reserv.dataloader.repository;

import com.reserv.dataloader.entity.ActivityLog;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ActivityLogRepo extends MongoRepository<ActivityLog, String> {
}