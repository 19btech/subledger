package com.reserv.dataloader.pulsar.producer;

import com.fyntrac.common.dto.record.Records;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class CommonMessageProducer {

    @Autowired
    private PulsarTemplate<Object> pulsarTemplate;

    @Value("${pulsar.topic.execute.model}")
    private String modelExecutiontopic;

    public void sendModelExecutionMessage(Records.InstrumentMessageRecord messageRecord) {
        var msgId = pulsarTemplate.send(modelExecutiontopic,messageRecord);
        log.info("EventPublisher::publishRawMessage publish topic {} {}, {}, {}", modelExecutiontopic,
                messageRecord.tenantId(), messageRecord.instrumentIds(), messageRecord.models());
        log.info("EventPublisher::publishRawMessage MessageId {}", msgId);
    }


}
