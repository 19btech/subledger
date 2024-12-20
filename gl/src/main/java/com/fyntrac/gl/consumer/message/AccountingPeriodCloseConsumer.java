package com.fyntrac.gl.consumer.message;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.gl.service.GeneralLedgerCommonService;
import com.fyntrac.gl.staging.ProcessGeneralLedgerStaging;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class AccountingPeriodCloseConsumer {

    private final ProcessGeneralLedgerStaging processGeneralLedgerStaging;
    private final GeneralLedgerCommonService generalLedgerCommonService;

    @Autowired
    public AccountingPeriodCloseConsumer(ProcessGeneralLedgerStaging processGeneralLedgerStaging
    ,GeneralLedgerCommonService generalLedgerCommonService) {
        this.processGeneralLedgerStaging = processGeneralLedgerStaging;
        this.generalLedgerCommonService = generalLedgerCommonService;
    }

    @PulsarListener(
            topics = "${spring.pulsar.producer.topic-accounting-period-close}",
            subscriptionName = "${spring.pulsar.consumer.subscription.name}",
            schemaType = SchemaType.JSON,
            subscriptionType = SubscriptionType.Shared
    )
    public void closeAccountingPeriod(Message<Records.AccountingPeriodCloseMessageRecord> message) {
        try {
            log.info("message {}", message);
            this.processGeneralLedgerStaging.closeAccountingPeriod(message.getValue());
            this.generalLedgerCommonService.processReclass(message.getValue());
        }catch (Exception exp){
            log.error(exp.getLocalizedMessage());
            throw exp;
        }
    }
}
