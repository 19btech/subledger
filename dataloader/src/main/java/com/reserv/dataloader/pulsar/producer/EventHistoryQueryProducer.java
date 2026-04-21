package com.reserv.dataloader.pulsar.producer;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.EventHistoryBatchTracker;
import com.fyntrac.common.repository.EventHistoryBatchTrackerRepository;
import com.fyntrac.common.config.TenantContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
public class EventHistoryQueryProducer {

    @Autowired
    private PulsarTemplate<Object> pulsarTemplate;
    
    @Autowired
    private EventHistoryBatchTrackerRepository trackerRepository;

    @Value("${spring.pulsar.producer.topic-event-history-query:persistent://public/default/event-history-query}")
    private String topic;

    /**
     * Publishes a batch of instrument IDs to query their EventHistory.
     * Overloaded method for quick defaults.
     */
    public void queryEventHistoryBatch(String tenantId, List<String> instrumentIds, Integer postingDate) {
        Long jobId = System.currentTimeMillis(); // or passed from caller
        queryEventHistoryBatch(tenantId, instrumentIds, jobId, postingDate);
    }

    /**
     * Publishes a batch query to the EventHistory py-model.
     * Note: instrumentIds should be chunked before calling this method to avoid
     * exceeding Pulsar's message size limit (typically around 500-1000 per message recommended).
     */
    public void queryEventHistoryBatch(String tenantId, List<String> instrumentIds, Long jobId, Integer postingDate) {
        if (instrumentIds == null || instrumentIds.isEmpty()) {
            log.warn("queryEventHistoryBatch called with empty instrumentIds list for tenant {}", tenantId);
            return;
        }

        String[] instrumentIdsArray = instrumentIds.toArray(new String[0]);
        Records.EventHistoryQueryRecord record = new Records.EventHistoryQueryRecord(tenantId, instrumentIdsArray, jobId, postingDate);

        // Track the job status
        TenantContextHolder.setTenant(tenantId);
        try {
            EventHistoryBatchTracker tracker = EventHistoryBatchTracker.builder()
                    .jobId(jobId)
                    .tenantId(tenantId)
                    .expectedInstrumentCount(instrumentIds.size())
                    .status("PRODUCED")
                    .createdAt(LocalDateTime.now())
                    .updatedAt(LocalDateTime.now())
                    .build();
            trackerRepository.save(tracker);
            log.info("Saved EventHistoryBatchTracker for JobId: {}", jobId);

            var msgId = pulsarTemplate.send(topic, record);
            log.info("EventHistoryQueryProducer::queryEventHistoryBatch published event. Tenant: {}, JobId: {}, BatchSize: {}", 
                     tenantId, jobId, instrumentIds.size());
            log.debug("EventHistoryQueryProducer MessageId {}", msgId);
        } catch (Exception e) {
            log.error("Failed to publish EventHistoryQueryRecord for Tenant: {}, JobId: {}", tenantId, jobId, e);
            throw new RuntimeException("Failed to publish query EventHistory batch", e);
        } finally {
            TenantContextHolder.clear();
        }
    }
}
