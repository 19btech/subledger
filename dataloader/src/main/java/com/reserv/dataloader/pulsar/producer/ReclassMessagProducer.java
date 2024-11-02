package com.reserv.dataloader.pulsar.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Service;
import com.fyntrac.common.dto.record.Records;

import java.util.UUID;

@Service
@Slf4j
public class ReclassMessagProducer {

    @Autowired
    private PulsarTemplate<Object> pulsarTemplate;

    @Value("${spring.pulsar.producer.topic-glReclass}")
    private String topic;

    public void sendReclassMessage(Records.ReclassMessageRecord reclassMessageRecord) {
        var msgId = pulsarTemplate.send(topic,reclassMessageRecord);
        log.info("EventPublisher::publishRawMessage publish the event {}, {}", reclassMessageRecord.tenantId(), reclassMessageRecord.dataKey());
        log.info("EventPublisher::publishRawMessage MessageId {}", msgId);
    }
}
