package com.reserv.dataloader.pulsar.producer;

import com.fyntrac.common.dto.record.Records;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ModelExecutionProducer {
    @Autowired
    private PulsarTemplate<Object> pulsarTemplate;

    @Value("${spring.pulsar.producer.topic-execute-model}")
    private String topic;

    public void sendModelExecutionMessage(Records.CommonMessageRecord messageRecord) {
        var msgId = pulsarTemplate.send(topic,messageRecord);
        log.info("EventPublisher::publishRawMessage publish topic {} {}, {}", topic, messageRecord.tenantId(), messageRecord.key());
        log.info("EventPublisher::publishRawMessage MessageId {}", msgId);
    }
}
