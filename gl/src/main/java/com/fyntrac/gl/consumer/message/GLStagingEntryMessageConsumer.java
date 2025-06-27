package com.fyntrac.gl.consumer.message;

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

    @PulsarListener(
            topics = "${spring.pulsar.producer.topic-bookGLStaging}",
            subscriptionName = "${spring.pulsar.consumer.subscription.name}",
            schemaType = SchemaType.JSON,
            subscriptionType = SubscriptionType.Shared
    )
    public void bookTempGL(Message<Records.GeneralLedgerMessageRecord> message) {
        try {
            Records.GeneralLedgerMessageRecord transactionActivity = message.getValue(); // Get the value from the message
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