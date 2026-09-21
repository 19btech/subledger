package com.reserv.dataloader.batch.listener;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fyntrac.common.service.BatchCompletionWaiter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.pulsar.annotation.PulsarListener;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class PythonModelCompletionListener {

    private final BatchCompletionWaiter batchCompletionWaiter;
    private final ObjectMapper objectMapper;

    @Autowired
    public PythonModelCompletionListener(BatchCompletionWaiter batchCompletionWaiter, ObjectMapper objectMapper) {
        this.batchCompletionWaiter = batchCompletionWaiter;
        this.objectMapper = objectMapper;
    }

    // Default (Exclusive) subscription lets only one consumer ever attach — fine for a single
    // instance, but with >1 dataloader replica the second pod's ConsumerBusyException fails its
    // whole ApplicationContext at startup (Spring's Pulsar listener container stops on startup
    // failure by default), so at most one replica could ever come up. Failover avoids that by
    // keeping exactly one active consumer at a time while letting standby replicas attach without
    // erroring.
    // Note: which replica's listener happens to be "active" no longer matters for correctness —
    // BatchCompletionWaiter now writes completions to Memcached (shared across all replicas)
    // instead of an in-process map, so Shared would work here too. The replica that dispatched a
    // given batch no longer needs to be the same one whose listener receives its completion;
    // see BatchCompletionWaiter for the bug that arose when it did.
    // Schema.BYTES, not JSON: the Python producer (fyntrac-py-model) sends this topic with no
    // Pulsar schema at all (client.create_producer(topic) + json.dumps(...).encode()), and
    // Pulsar locks a freshly-created topic's schema to whichever side — this Java consumer or
    // that Python producer — touches it first. Since Pulsar's local dev data is wiped on every
    // restart (see pulsar.yaml's reset-pulsar-data initContainer), that race replays on every
    // restart too, and losing it (Python touches the topic first) permanently blocks this
    // consumer with IncompatibleSchemaException until the topic is wiped again. Consuming raw
    // bytes and parsing JSON ourselves sidesteps Pulsar's schema registry entirely, so it never
    // conflicts with the schemaless producer regardless of who gets there first.
    @PulsarListener(topics = "${spring.pulsar.consumer.topic-python-model-completion:fyntrac-python-model-completion}",
            subscriptionType = SubscriptionType.Failover,
            schemaType = SchemaType.BYTES)
    public void onPythonModelCompletion(Message<byte[]> message) throws java.io.IOException {
        String correlationId = message.getProperty("correlationId");

        if (correlationId == null || correlationId.isBlank()) {
            log.error("Received Python completion message without correlationId. Raw bytes length: {}",
                    message.getValue() == null ? 0 : message.getValue().length);
            return;
        }

        Map<String, Object> payload = objectMapper.readValue(message.getValue(), new TypeReference<Map<String, Object>>() {});
        boolean success = Boolean.TRUE.equals(payload.get("success"));
        String resultData = (String) payload.get("result");
        String error = (String) payload.get("error");

        log.info("Python completion received for correlationId: {}. Success: {}", correlationId, success);

        if (!success) {
            // error is frequently null here when the Python service reports a failure without
            // populating the "error" field (or uses a different field/type than expected) — log
            // the raw payload so the actual cause isn't lost behind a bare "null" message upstream.
            log.error("Python completion FAILED for correlationId: {}. Raw payload: {}", correlationId, payload);
            if (error == null || error.isBlank()) {
                error = "Python service reported failure with no error detail. Raw payload: " + payload;
            }
        }

        batchCompletionWaiter.completeBatch(
                correlationId,
                new BatchCompletionWaiter.BatchResult(
                        success ? "SUCCESS" : "FAILED",
                        resultData,
                        error
                )
        );
    }
}
