package com.fyntrac.model.pulsar.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class ModelCompletionProducer {

    @Autowired
    private PulsarTemplate<Object> pulsarTemplate;

    // Use the same topic that dataloader listens to for completion callbacks
    @Value("${spring.pulsar.producer.topic-model-completion:fyntrac-python-model-completion}")
    private String topic;

    public void sendCompletionMessage(String correlationId, boolean success, String resultPayload, String errorMsg) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("success", success);
        payload.put("result", resultPayload);
        payload.put("error", errorMsg);

        try {
            var msgId = pulsarTemplate.newMessage(payload)
                    .withTopic(topic)
                    .withMessageCustomizer(mb -> mb.property("correlationId", correlationId))
                    .send();

            log.info("ModelCompletionProducer::sendCompletion correlationId={} success={} msgId={}",
                    correlationId, success, msgId);
        } catch (Exception e) {
            log.error("Failed to send completion message for correlationId: {}", correlationId, e);
        }
    }
}
