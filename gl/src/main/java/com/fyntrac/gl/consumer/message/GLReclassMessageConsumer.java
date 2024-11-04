package com.fyntrac.gl.consumer.message;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.gl.service.GeneralLedgerReclassService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class GLReclassMessageConsumer {

    private GeneralLedgerReclassService generalLedgerReclassService;

    @Autowired
    public GLReclassMessageConsumer(GeneralLedgerReclassService generalLedgerReclassService){
        this.generalLedgerReclassService = generalLedgerReclassService;
    }
    @PulsarListener(
            topics = "${spring.pulsar.producer.topic-glReclass}",
            subscriptionName = "${spring.pulsar.consumer.subscription.name}",
            schemaType = SchemaType.JSON,
            subscriptionType = SubscriptionType.Shared
    )
    public void checkReclass(Message<Records.ReclassMessageRecord> message) throws ExecutionException, InterruptedException {
        Map<String, Object> executionContext = new HashMap<>(0);
        Records.ReclassMessageRecord reclassMessageRecord = message.getValue();
        log.info("Reclass message {}", reclassMessageRecord);
        executionContext.put("tenantId", reclassMessageRecord.tenantId());
        executionContext.put("dataKey", reclassMessageRecord.dataKey());
        generalLedgerReclassService.execute(executionContext);
    }
}
