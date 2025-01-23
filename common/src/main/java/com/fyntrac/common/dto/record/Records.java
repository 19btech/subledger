package com.fyntrac.common.dto.record;

import com.fyntrac.common.entity.Batch;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.entity.Model;
import com.fyntrac.common.entity.ModelFile;
import com.fyntrac.common.enums.UploadStatus;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serializable;
import java.util.Collection;
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
) implements Serializable {
        private static final long serialVersionUID = 3287905346762365888L; // Optional, but good practice
    }

    public record ReclassMessageRecord(String tenantId, String dataKey)implements Serializable {
        private static final long serialVersionUID = -8149874708782902606L; // Optional, but good practice
    }

    public record InstrumentAttributeReclassMessageRecord(String tenantId
            , long batchId
            , InstrumentAttributeRecord previousInstrumentAttribute
            , InstrumentAttributeRecord currentInstrumentAttribute) implements Serializable {
        private static final long serialVersionUID = -8473589388962080923L; // Optional, but good practice
    }

    public record AccountingPeriodCloseMessageRecord(String tenantId, Collection<Batch> batches)implements Serializable {
        private static final long serialVersionUID = 322984303560312158L;
    }

    public record MetricNameRecord(String metricName)implements Serializable {
        private static final long serialVersionUID = -4338320429519961695L;
    }

    public record TransactionNameRecord(String transactionName)implements Serializable {
        private static final long serialVersionUID = -4338320429519961695L;
    }

    public record CommonMessageRecord(String tenantId, String key)implements Serializable {
        private static final long serialVersionUID = -1788629874681694218L;
    }

    public record ModelRecord(Model model, Workbook workbook)implements Serializable {
        private static final long serialVersionUID = -8716704041115418296L;
    }
}