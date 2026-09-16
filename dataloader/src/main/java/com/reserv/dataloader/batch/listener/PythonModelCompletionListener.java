package com.reserv.dataloader.batch.listener;

import com.fyntrac.common.service.BatchCompletionWaiter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.pulsar.annotation.PulsarListener;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class PythonModelCompletionListener {

    private final BatchCompletionWaiter batchCompletionWaiter;

    @Autowired
    public PythonModelCompletionListener(BatchCompletionWaiter batchCompletionWaiter) {
        this.batchCompletionWaiter = batchCompletionWaiter;
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
    @PulsarListener(topics = "${spring.pulsar.consumer.topic-python-model-completion:fyntrac-python-model-completion}",
            subscriptionType = SubscriptionType.Failover)
    public void onPythonModelCompletion(Message<Map<String, Object>> message) {
        String correlationId = message.getProperty("correlationId");

        if (correlationId == null || correlationId.isBlank()) {
            log.error("Received Python completion message without correlationId. Payload: {}", message.getValue());
            return;
        }

        Map<String, Object> payload = message.getValue();
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
