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
public class AccountingPeriodCloseProducer {
    @Autowired
    private PulsarTemplate<Object> pulsarTemplate;

    @Value("${spring.pulsar.producer.topic-accounting-period-close}")
    private String topic;

    public void closeAccountingPeriod(Records.AccountingPeriodCloseMessageRecord accountingPeriodCloseMessageRecord){
        var msgId = pulsarTemplate.send(topic, accountingPeriodCloseMessageRecord);
        log.info("EventPublisher::publishRawMessage publish the event {}, {}", accountingPeriodCloseMessageRecord.tenantId(), accountingPeriodCloseMessageRecord.batches());
        log.info("EventPublisher::publishRawMessage MessageId {}", msgId);
    }
}
