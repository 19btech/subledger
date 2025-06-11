package com.reserv.dataloader.pulsar.consumer;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.utils.DateUtil;
import com.reserv.dataloader.service.AggregationExecutionService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
@Slf4j
public class ExecuteAggregationConsumer {
    @Value("${spring.pulsar.producer.topic-execute-aggregation}")
    private String topic;

    @Value("${spring.pulsar.consumer.subscription.name}")
    private String subscription;

    @Value("${spring.pulsar.client.service-url}")
    private String pulsarURL;

    @Autowired
    private AggregationExecutionService aggregationExecutionService;

    @Autowired
    private Job updateInstrumentActivitySateJob;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    ExecutionStateService executionStateService;

    private PulsarClient client;
    private Consumer<Records.ExecuteAggregationMessageRecord> consumer;


    @PostConstruct
    public void init() throws PulsarClientException {
        // Step 1: Create a Pulsar client
        client = PulsarClient.builder()
                .serviceUrl(pulsarURL) // Replace with your Pulsar broker URL
                .build();

        // Step 2: Create a consumer
        consumer = client.newConsumer(Schema.JSON(Records.ExecuteAggregationMessageRecord.class))
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
                Message<Records.ExecuteAggregationMessageRecord> msg = consumer.receive();

                // Step 5: Process the message
                processMessage(msg);
            } catch (Throwable e) {
                log.error("Failed to process message", e);
            }
        }
    }

    private void processMessage(Message<Records.ExecuteAggregationMessageRecord> msg) throws Throwable {
        try {
            // Step 6: Set tenant context
            TenantContextHolder.setTenant(msg.getValue().tenantId());

            // Step 7: Execute the model
            Records.ExecuteAggregationMessageRecord msgRec = msg.getValue();


            ExecutionState executionState = this.executionStateService.getExecutionState();

            executionState.setLastExecutionDate(executionState.getLastExecutionDate());
            if(executionState.getExecutionDate() != null
                    && msgRec.aggregationDate() > executionState.getExecutionDate()) {
                executionState.setLastExecutionDate(executionState.getExecutionDate());
            }
            executionState.setExecutionDate(msgRec.aggregationDate().intValue());

            aggregationExecutionService.execute(msg.getValue(), executionState);

            /**
             * update instrument state for last effective date to set the replay mark
             * for that instrument
             */
            Long runId = System.currentTimeMillis();
            Records.ExecuteAggregationMessageRecord messageRecord = msg.getValue();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("run.id", runId)
                    .addLong("execution-date", messageRecord.aggregationDate())
                    .toJobParameters();
            jobLauncher.run(updateInstrumentActivitySateJob, jobParameters);

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
            // TenantContextHolder.clear();
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
