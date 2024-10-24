package com.fyntrac.gl.consumer.config;

import com.fyntrac.common.dto.record.Records;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.client.api.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarConfig {

    @Value("${spring.pulsar.client.service-url}")
    private String pulsarHost;

    @Value("${spring.pulsar.producer.topic-bookGLStaging}")
    private String topiName;

    @Value("${spring.pulsar.consumer.subscription.name}")
    private String subscriptionName;
    @Bean
    public PulsarClient pulsarClient() throws Exception {
        return PulsarClient.builder()
                .serviceUrl(pulsarHost) // Replace with your Pulsar service URL
                .build();
    }

    @Bean
    public Consumer<Records.TransactionActivityRecord> transactionActivityConsumer(PulsarClient pulsarClient) throws Exception {
        return pulsarClient.newConsumer(Schema.JSON(Records.TransactionActivityRecord.class))
                .topic(topiName) // Replace with your topic name
                .subscriptionName(subscriptionName) // Replace with your subscription name
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
    }
}