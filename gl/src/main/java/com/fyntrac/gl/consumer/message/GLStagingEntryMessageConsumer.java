package com.fyntrac.gl.consumer.message;

import com.fyntrac.common.dto.record.Records;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j

public class GLStagingEntryMessageConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private Consumer<Records.TransactionActivityRecord> transactionActivityConsumer; // Inject the Pulsar consumer

    @PulsarListener(
            topics = "${spring.pulsar.producer.topic-bookGLStaging}",
            subscriptionName = "${spring.pulsar.consumer.subscription.name}",
            schemaType = SchemaType.JSON,
            subscriptionType = SubscriptionType.Shared
    )
    public void bookTempGL(Message<Records.TransactionActivityRecord> message) throws JsonProcessingException {
        try {
            Records.TransactionActivityRecord transactionActivity = message.getValue(); // Get the value from the message
            // Process the message
            log.info("EventConsumer:: consumeTextEvent consumed events {}", transactionActivity.toString());

            // Acknowledge the message after successful processing
            transactionActivityConsumer.acknowledge(message); // Acknowledge using the consumer
        } catch (Exception e) {
            // Handle any exceptions that occur during processing
            log.error("Error processing message: {}", e.getMessage());
            // Optionally, you can handle retries or dead-lettering here
        }
    }
}
