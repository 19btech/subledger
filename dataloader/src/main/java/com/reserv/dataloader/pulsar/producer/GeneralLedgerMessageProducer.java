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
public class GeneralLedgerMessageProducer {

    @Autowired
    private PulsarTemplate<Object> pulsarTemplate;

    @Value("${spring.pulsar.producer.topic-bookGLStaging}")
    private String topic;

    public void bookTempGL(Records.GeneralLedgerMessageRecord glActivity){
        String traceId = UUID.randomUUID().toString();
        var msgId = pulsarTemplate.send(topic,glActivity);
        log.info("EventPublisher::publishRawMessage publish the event {}, {}", glActivity.tenantId(), glActivity.jobId());
        log.info("EventPublisher::publishRawMessage MessageId {}", msgId);
    }
}
