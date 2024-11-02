package com.fyntrac.common.dto.record;

import com.fyntrac.common.entity.InstrumentAttribute;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.Map;

public class Records {
    // Record definition for Accounting Period
    public record AccountingPeriodRecord(int periodId, String period, int fiscalPeriod, int year, int status) {
    }

    public record GeneralLedgerMessageRecord(String tenantId, String dataKey){}
    public record TransactionActivityRecord(
            String tenantId,
            String id,
            Date transactionDate,
            String instrumentId,
            String transactionName,
            double value,
            String attributeId,
            int periodId,
            int originalPeriodId
    ) {
        // No additional methods are needed unless you want to add custom behavior
    }

    public record InstrumentAttributeRecord(Date effectiveDate,
            String instrumentId,
            String attributeId,
            Date endDate,
            int periodId,
            long versionId,
            Map<String,Object> attributes
) {}

    public record ReclassMessageRecord(String tenantId, String dataKey){}

    public record InstrumentAttributeReclassMessageRecord(String tenantId
            , InstrumentAttributeRecord previousInstrumentAttribute
            , InstrumentAttributeRecord currentInstrumentAttribute) {}
}