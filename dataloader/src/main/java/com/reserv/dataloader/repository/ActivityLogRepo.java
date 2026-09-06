package com.reserv.dataloader.repository;

import com.reserv.dataloader.entity.ActivityLog;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ActivityLogRepo extends MongoRepository<ActivityLog, String> {

    // Removes the load-tracking record(s) for a posting date whose activity data was purged,
    // so the Settings page's "latest loaded posting date" recalculates correctly afterward.
    long deleteByPostingDate(Integer postingDate);
}