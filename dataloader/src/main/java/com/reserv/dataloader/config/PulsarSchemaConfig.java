package com.reserv.dataloader.config;

import com.fyntrac.common.dto.record.Records;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class PulsarSchemaConfig {

    private final PulsarAdmin pulsarAdmin;
    private final String topic;

    public PulsarSchemaConfig(@Value("${spring.pulsar.client.service-url}") String pulsarHttpUrl,
                              @Value("${spring.pulsar.producer.topic-execute-aggregation}") String topic)
            throws PulsarAdminException, PulsarClientException {
        this.pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarHttpUrl)  // Must be HTTP URL like http://localhost:8080
                .build();
        this.topic = topic;
    }

    @PostConstruct
    public void configureSchemaCompatibility() {
        try {
            String topicFQN = String.format("persistent://public/default/%s", topic);
            pulsarAdmin.schemas().createSchema(topicFQN, (SchemaInfo) Schema.AVRO(Records.ExecuteAggregationMessageRecord.class));

            pulsarAdmin.namespaces().setSchemaCompatibilityStrategy(
                    "public/default", SchemaCompatibilityStrategy.FORWARD);
        } catch (PulsarAdminException e) {
            throw new RuntimeException("Failed to configure Pulsar schema", e);
        }
    }
}
