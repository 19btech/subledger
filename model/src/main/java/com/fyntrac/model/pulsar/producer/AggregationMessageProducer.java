package com.fyntrac.model.pulsar.producer;

import com.fyntrac.common.dto.record.Records;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
public class AggregationMessageProducer {

    @Autowired
    private PulsarTemplate<Object> pulsarTemplate;

    @Value("${spring.pulsar.producer.topic-execute-aggregation}")
    private String topic;

    public void executeAggregation(Records.ExecuteAggregationMessageRecord aggregationMessage){
        String traceId = UUID.randomUUID().toString();
        var msgId = pulsarTemplate.send(topic,aggregationMessage);
        log.info("EventPublisher::publishRawMessage publish the event [executeAggregation] {}, {}", aggregationMessage.aggregationDate(), aggregationMessage.jobId());
        log.info("EventPublisher::publishRawMessage MessageId {}", msgId);
    }
}
