package com.fyntrac.gl.consumer.message;

import com.fyntrac.common.dto.record.Records;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class GLReclassMessageConsumer {

    @PulsarListener(
            topics = "${spring.pulsar.producer.topic-glReclass}",
            subscriptionName = "${spring.pulsar.consumer.subscription.name}",
            schemaType = SchemaType.JSON,
            subscriptionType = SubscriptionType.Shared
    )
    public void checkReclass(Message<Records.ReclassMessageRecord> message) {
        log.info("Reclass message {}", message.getValue());

    }
}
