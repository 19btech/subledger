package com.fyntrac.model.consumer.message;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.model.service.ModelExecutionService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
@Slf4j
public class ModelExecutionConsumer {

    @Value("${spring.pulsar.producer.topic-execute-model}")
    private String topic;

    @Value("${spring.pulsar.consumer.subscription.name}")
    private String subscription;

    @Value("${spring.pulsar.client.service-url}")
    private String pulsarURL;

    @Autowired
    private ModelExecutionService modelExecutionService;


    private PulsarClient client;
    private Consumer<Records.ModelExecutionMessageRecord> consumer;

    @PostConstruct
    public void init() throws PulsarClientException {
        // Step 1: Create a Pulsar client
        client = PulsarClient.builder()
                .serviceUrl(pulsarURL) // Replace with your Pulsar broker URL
                .build();

        // Step 2: Create a consumer
        consumer = client.newConsumer(Schema.JSON(Records.ModelExecutionMessageRecord.class))
                .topic(topic) // Replace with your topic name
                .subscriptionName(subscription) // Replace with your subscription name
                .subscriptionType(SubscriptionType.Shared) // Use Shared or Failover subscription
                .subscribe();

        // Step 3: Start consuming messages in a separate thread
        new Thread(this::consumeMessages).start();
    }

    private void consumeMessages() {
        while (true) {
            try {
                // Step 4: Receive the next message
                Message<Records.ModelExecutionMessageRecord> msg = consumer.receive();

                // Step 5: Process the message
                processMessage(msg);
            } catch (Throwable e) {
                log.error("Failed to process message", e);
            }
        }
    }

    private void processMessage(Message<Records.ModelExecutionMessageRecord> msg) throws Throwable {
        try {
            // Step 6: Set tenant context
            TenantContextHolder.setTenant(msg.getValue().tenantId());

            // Step 7: Execute the model
            Date executionDate = DateUtil.convertToDateFromYYYYMMDD(msg.getValue().executionDate());
            modelExecutionService.executeModels(executionDate, msg.getValue());

            // Step 8: Log success
            log.info("Message processed successfully: {}", msg.getValue());

            // Step 9: Acknowledge the message
            consumer.acknowledge(msg);
        } catch (Exception exp) {
            // Step 10: Log the error
            log.error("Error processing message: {}", msg.getValue(), exp);

            // Step 11: Handle the exception
            if (isRetryableException(exp)) {
                // If the exception is retryable, negatively acknowledge the message
                consumer.negativeAcknowledge(msg);
            } else {
                // If the exception is non-retryable, acknowledge the message to prevent redelivery
                log.warn("Non-retryable exception occurred. Acknowledging message to avoid redelivery: {}", msg.getValue());
                consumer.acknowledge(msg);
            }

            // Step 12: Re-throw the exception if needed
            throw exp;
        } finally {
            // Step 13: Clear tenant context (if necessary)
            TenantContextHolder.clear();
        }
    }

    private boolean isRetryableException(Exception exp) {
        // Add logic to determine if the exception is retryable
        // For example, network-related exceptions might be retryable, while validation errors might not be
        return !(exp instanceof RuntimeException);
    }

    @PreDestroy
    public void cleanup() throws PulsarClientException {
        // Step 14: Close the consumer and client when the application shuts down
        if (consumer != null) {
            consumer.close();
        }
        if (client != null) {
            client.close();
        }
    }
}

