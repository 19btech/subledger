package com.fyntrac.common.dto.record;

import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.Batch;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.entity.TransactionActivity;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.function.Supplier;

@Component
public class RecordFactory {

    // Generic factory method that takes a Supplier for any record type
    public static <T> T createRecord(Supplier<T> constructor) {
        return constructor.get();
    }

    // Specific methods for creating records
    public static Records.AccountingPeriodRecord createAccountingPeriodRecord(AccountingPeriod accountingPeriod) {
        if(accountingPeriod == null) {
            return createRecord(() -> new Records.AccountingPeriodRecord(
                    0
                    , "_ _ / _ _"
                    , 0
                    , 0
                    , 0));
        }else {
            return createRecord(() -> new Records.AccountingPeriodRecord(
                    accountingPeriod.getPeriodId()
                    , accountingPeriod.getPeriod()
                    , accountingPeriod.getFiscalPeriod()
                    , accountingPeriod.getYear()
                    , accountingPeriod.getStatus()));
        }
    }

    public static Records.GeneralLedgerMessageRecord createGeneralLedgerMessageRecord(String tenantId, String dataKey){
        return createRecord(()->new Records.GeneralLedgerMessageRecord(tenantId, dataKey));
    }

    public static Records.TransactionActivityRecord createTransactionActivityRecord(TransactionActivity transactionActivity, String tenantId){
        return createRecord(() -> new Records.TransactionActivityRecord(
                tenantId,
                transactionActivity.getId(),
                transactionActivity.getTransactionDate(),
                transactionActivity.getInstrumentId(),
                transactionActivity.getTransactionName(),
                transactionActivity.getAmount(),
                transactionActivity.getAttributeId(),
                transactionActivity.getPeriodId(),
                transactionActivity.getOriginalPeriodId()));
    }

    public static Records.InstrumentAttributeRecord createInstrumentAttributeRecord(InstrumentAttribute instrumentAttribute){
        return createRecord(()-> new Records.InstrumentAttributeRecord(instrumentAttribute.getEffectiveDate()
                , instrumentAttribute.getInstrumentId()
                , instrumentAttribute.getAttributeId()
                , instrumentAttribute.getEndDate()
                , instrumentAttribute.getPeriodId()
                , instrumentAttribute.getVersionId()
                , instrumentAttribute.getAttributes()

        ));
    }

    public static Records.InstrumentAttributeReclassMessageRecord createInstrumentAttributeReclassMessageRecord(
            String tenantId
            , long batchId
            , Records.InstrumentAttributeRecord previousInstrumentAttribute
            , Records.InstrumentAttributeRecord currentInstrumentAttribute
    ) {
        return createRecord(() -> new Records.InstrumentAttributeReclassMessageRecord(tenantId, batchId, previousInstrumentAttribute, currentInstrumentAttribute));
    }

    public static Records.ReclassMessageRecord createReclassMessageRecord(String tenantId, String dataKey) {
        return createRecord(()-> new Records.ReclassMessageRecord(tenantId, dataKey));
    }

    public static Records.AccountingPeriodCloseMessageRecord createAccountingPeriodCloseMessage(String tenant, Collection<Batch> batches){
        return createRecord(() -> new Records.AccountingPeriodCloseMessageRecord(tenant, batches));
    }

    public static Records.CommonMessageRecord createCommonMessage(String tenant, String key){
        return createRecord(() -> new Records.CommonMessageRecord(tenant, key));
    }
}
