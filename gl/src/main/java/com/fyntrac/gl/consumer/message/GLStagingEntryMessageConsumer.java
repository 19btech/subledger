package com.fyntrac.gl.consumer.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.gl.staging.ProcessGeneralLedgerStaging;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class GLStagingEntryMessageConsumer {

    @Autowired
    ProcessGeneralLedgerStaging processGeneralLedgerStaging;

    @Autowired
    ObjectMapper objectMapper;

    // Schema.BYTES, not JSON: fyntrac-py-model's GeneralLedgerMessageProducer sends this topic
    // with no Pulsar schema at all (client.create_producer(topic) + json.dumps(...).encode()),
    // and Pulsar locks a freshly-created topic's schema to whichever side touches it first. Since
    // Pulsar's local dev data is wiped on every restart (see pulsar.yaml's reset-pulsar-data
    // initContainer), that race replays on every restart — losing it (Python touches the topic
    // first) permanently blocks this consumer with IncompatibleSchemaException until the next
    // wipe. Consuming raw bytes and parsing JSON ourselves sidesteps Pulsar's schema registry
    // entirely, so it never conflicts with the schemaless producer regardless of who wins.
    @PulsarListener(
            topics = "${spring.pulsar.producer.topic-bookGLStaging}",
            subscriptionName = "${spring.pulsar.consumer.subscription.name}",
            schemaType = SchemaType.BYTES,
            subscriptionType = SubscriptionType.Shared
    )
    public void bookTempGL(Message<byte[]> message) {
        try {
            Records.GeneralLedgerMessageRecord transactionActivity =
                    objectMapper.readValue(message.getValue(), Records.GeneralLedgerMessageRecord.class);
            // Process the message
            log.info("EventConsumer:: consumeTextEvent consumed events {}, {}", transactionActivity.tenantId(), transactionActivity.jobId());
            // No need to acknowledge here; @PulsarListener handles it
            processGeneralLedgerStaging.process(transactionActivity);
        } catch (Exception e) {
            // Handle any exceptions that occur during processing
            log.error("Error processing message: {}", e.getMessage(), e); // Log the exception stack trace
            // Optionally, implement retry logic or send to a dead-letter topic
        }
    }
}