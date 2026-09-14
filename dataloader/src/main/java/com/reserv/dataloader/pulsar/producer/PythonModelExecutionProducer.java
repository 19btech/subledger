package com.reserv.dataloader.pulsar.producer;

import com.fyntrac.common.dto.record.Records;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PythonModelExecutionProducer {
    @Autowired
    private PulsarTemplate<Object> pulsarTemplate;

    @Value("${spring.pulsar.producer.topic-execute-python-model}")
    private String topic;

    public void sendPythonModelExecutionMessage(Records.PythonModelExecutionMessageRecord messageRecord) {
        var msgId = pulsarTemplate.send(topic, messageRecord);
        log.info("PythonModelExecutionProducer::send topic={} tenant={} ",
                topic, messageRecord.tenantId());
        log.info("PythonModelExecutionProducer::send MessageId {}", msgId);
    }

    public void sendPythonModelExecutionMessageOrchestrated(Records.PythonModelExecutionMessageRecord messageRecord, String correlationId) {
        var msgId = pulsarTemplate.newMessage(messageRecord)
                .withTopic(topic)
                .withMessageCustomizer(mb -> mb.property("correlationId", correlationId))
                .send();
        log.info("PythonModelExecutionProducer::sendOrchestrated topic={} tenant={} correlationId={} msgId={}",
                topic, messageRecord.tenantId(), correlationId, msgId);
    }
}
