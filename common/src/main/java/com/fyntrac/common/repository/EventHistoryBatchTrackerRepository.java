package com.fyntrac.common.repository;

import com.fyntrac.common.entity.EventHistoryBatchTracker;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EventHistoryBatchTrackerRepository extends MongoRepository<EventHistoryBatchTracker, String> {
    
    EventHistoryBatchTracker findByTenantIdAndJobId(String tenantId, Long jobId);
    
}
