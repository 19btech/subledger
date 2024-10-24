package com.reserv.dataloader.records;

import com.fyntrac.common.entity.AccountingPeriod;
import org.springframework.stereotype.Component;
import com.reserv.dataloader.records.Records.AccountingPeriodRecord;
import java.util.function.Supplier;

@Component
public class RecordFactory {

    // Generic factory method that takes a Supplier for any record type
    public static <T> T createRecord(Supplier<T> constructor) {
        return constructor.get();
    }

    // Specific methods for creating records
    public static AccountingPeriodRecord createAccountingPeriodRecord(AccountingPeriod accountingPeriod) {
        if(accountingPeriod == null) {
            return createRecord(() -> new AccountingPeriodRecord(
                    0
                    , "_ _ / _ _"
                    , 0
                    , 0
                    , 0));
        }else {
            return createRecord(() -> new AccountingPeriodRecord(
                    accountingPeriod.getPeriodId()
                    , accountingPeriod.getPeriod()
                    , accountingPeriod.getFiscalPeriod()
                    , accountingPeriod.getYear()
                    , accountingPeriod.getStatus()));
        }
    }
}
