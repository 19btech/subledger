package com.reserv.dataloader.batch.listener;

import com.fyntrac.common.service.BatchCompletionWaiter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.pulsar.annotation.PulsarListener;
import org.apache.pulsar.client.api.Message;
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

    @PulsarListener(topics = "${spring.pulsar.consumer.topic-python-model-completion:fyntrac-python-model-completion}")
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
