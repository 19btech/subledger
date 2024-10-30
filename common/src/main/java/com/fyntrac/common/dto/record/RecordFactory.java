package com.fyntrac.common.dto.record;

import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.TransactionActivity;
import org.springframework.stereotype.Component;
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
}
