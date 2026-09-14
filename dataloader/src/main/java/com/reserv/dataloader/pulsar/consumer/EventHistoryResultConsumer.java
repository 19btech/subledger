package com.reserv.dataloader.pulsar.consumer;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EventHistoryResultConsumer {

    @Value("${spring.pulsar.consumer.topic-event-history-result:persistent://public/default/event-history-result}")
    private String topic;

    @Value("${spring.pulsar.consumer.subscription.eh-result-sub:event-history-result-subscription}")
    private String subscription;

    @Value("${spring.pulsar.client.service-url:pulsar://localhost:6650}")
    private String pulsarURL;

    private PulsarClient client;
    private Consumer<Records.EventHistoryResultRecord> consumer;

    @PostConstruct
    public void init() {
        try {
            client = PulsarClient.builder()
                    .serviceUrl(pulsarURL)
                    .build();

            consumer = client.newConsumer(Schema.JSON(Records.EventHistoryResultRecord.class))
                    .topic(topic)
                    .subscriptionName(subscription)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();

            new Thread(this::consumeMessages).start();
        } catch (PulsarClientException e) {
            log.error("EventHistoryResultConsumer: Failed to initialize — consumer disabled. " +
                    "If this is a schema incompatibility, delete the topic/schema via Pulsar admin and restart. " +
                    "Error: {}", e.getMessage());
            // Do NOT rethrow — prevents a stale Pulsar schema from crashing the entire application context.
            // The rest of the application (HTTP endpoints, other consumers) will still function normally.
        }
    }

    private void consumeMessages() {
        while (true) {
            try {
                Message<Records.EventHistoryResultRecord> msg = consumer.receive();
                processMessage(msg);
            } catch (Throwable e) {
                log.error("EventHistoryResultConsumer: Failed to receive/process message", e);
            }
        }
    }

    private void processMessage(Message<Records.EventHistoryResultRecord> msg) throws Throwable {
        try {
            Records.EventHistoryResultRecord resultRecord = msg.getValue();
            
            // Set tenant context
            TenantContextHolder.setTenant(resultRecord.tenantId());

            if (!resultRecord.success()) {
                log.error("Received failed EventHistory result. Tenant: {}, JobId: {}, Error: {}", 
                        resultRecord.tenantId(), resultRecord.jobId(), resultRecord.error());
            } else {
                log.info("Received successful EventHistory result. Tenant: {}, JobId: {}, Batch Results Retrieved: {}, CacheKey: {}", 
                        resultRecord.tenantId(), resultRecord.jobId(), resultRecord.resultCount(), resultRecord.cacheKey());
                
                // TODO: Fetch from Memcached using resultRecord.cacheKey()
                // and proceed with processing logic.
            }

            // Acknowledge the message upon successful processing
            consumer.acknowledge(msg);
            
        } catch (Exception exp) {
            log.error("Error processing EventHistoryResult message: {}", msg.getValue(), exp);
            consumer.negativeAcknowledge(msg); // Let Pulsar redeliver 
        } finally {
            TenantContextHolder.clear();
        }
    }

    @PreDestroy
    public void cleanup() throws PulsarClientException {
        if (consumer != null) {
            consumer.close();
        }
        if (client != null) {
            client.close();
        }
    }
}
