package com.fyntrac.common.dto.record;

import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.InstrumentAttributeVersionType;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.enums.TestStep;
import com.fyntrac.common.enums.UploadStatus;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.format.annotation.NumberFormat;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class Records {
    // Record definition for Accounting Period
    public record AccountingPeriodRecord(int periodId, String period, int fiscalPeriod, int year, int status) {
    }

    public record GeneralLedgerMessageRecord(String tenantId, Long jobId) implements Serializable {
        private static final long serialVersionUID = 1L;

        // Factory method with default values
        public static GeneralLedgerMessageRecord withDefaults(String tenantId, Long jobId) {
            return new GeneralLedgerMessageRecord(
                    tenantId != null ? tenantId : "default-tenant",
                    jobId != null ? jobId : 0L
            );
        }
    }
    public record TransactionActivityRecord(
            String tenantId,
            String id,
            Date transactionDate,
            String instrumentId,
            String transactionName,
            BigDecimal value,
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

    public record ModelExecutionMessageRecord(String tenantId, Integer executionDate, String key)implements Serializable {
        private static final long serialVersionUID = -1788629874681694218L;
    }

    public record ModelRecord(Model model, ModelFile modelFile)implements Serializable {
        private static final long serialVersionUID = -8716704041115418296L;
    }

    public record ExcelModelRecord(Model model, Workbook excelModel) implements Serializable {
        private static final long serialVersionUID = 58415234907355427L;
    }

    public record MetricRecord(String MetricName, String Instrumentid, String attributeId, int AccountingPeriod, Date postingDate, BigDecimal BeginningBalance, BigDecimal Activity, BigDecimal EndingBalance) implements Serializable {
        private static final long serialVersionUID = -1801701397561747928L;
    }

    public record DateRequestRecord(String date) implements Serializable {
        private static final long serialVersionUID = 3196156514121109291L;
    }

    public record ModelOutputData(List<Map<String, Object>> transactionActivityList, List<Map<String, Object>> instrumentAttributeList)  implements  Serializable{
        private static final long serialVersionUID = 721671730137381082L;
    }

    public record DocumentAttribute(String attributeName, String attributeAlias, String dataType) implements Serializable{
        private static final long serialVersionUID = 8318251040744803358L;
    }

    public record QueryCriteriaItem(String attributeName, String operator,String values, List<String> filters, String logicalOperator) implements Serializable{
        private static final long serialVersionUID = -6100862855198499095L;
    }

    public record TransactionActivityReplayRecord(String instrumentId, String attributeId, Integer replayDate) implements Serializable{
        private static final long serialVersionUID = 3735691512748555397L;
    }

    public record TransactionActivityReversalRecord(String instrumentId, String attributeId, String transactionType, int effectiveDate, BigDecimal totalAmount, Map<String, Object> attributes, long instrumentAttributeVersionId, int originalPeriodId, Date transactionDate, AccountingPeriod accountingPeriod, long batchId) implements Serializable{
        private static final long serialVersionUID = -8661375155752282087L;
    }

    public record ExecutionDateRecord(Date executionDate, Date	lastExecutionDate, Date	replayDate) implements Serializable{
        private static final long serialVersionUID = -8940702485412412978L;
    }

    public record ExecuteAggregationMessageRecord(String tenantId, Long jobId, Long aggregationDate) implements Serializable {
        private static final long serialVersionUID = -588100000724731968L;

        public static ExecuteAggregationMessageRecord withDefaults(String tenantId, Long jobId, Long aggregationDate) {
            return new ExecuteAggregationMessageRecord(
                    tenantId != null ? tenantId : "default-tenant",
                    jobId != null ? jobId : 0L,
                    aggregationDate
            );
        }
    }
    public record GroupedMetricsByInstrumentAttribute(String instrumentId,
            String attributeId, // Assuming attributeId is String - adjust if needed
            String metricName) {
        private static final long serialVersionUID = 8087138613941001670L;
    }

    public record GroupedMetricsByInstrument(String instrumentId,
                                                      String metricName) {
        private static final long serialVersionUID = -7455631766826985863L;
    }

    public record ExcelTestStepRecord(TestStep step, String typ, String input){
        private static final long serialVersionUID = -4174200146849813549L;
    }

    /**
     * Record class holding the row number and corresponding metricName and attributeId values for a row.
     */
    public record MetricAttributeRow(int rowNumber, String metricName, String attributeId) {
        private static final long serialVersionUID = 5618122846381141806L;
    }

    /**
     * Record class holding the row number and corresponding transactionName and attributeId values for a row.
     */
    public record TransactionAttributeRow(int rowNumber, String transactionName, String attributeId) {
        private static final long serialVersionUID = -2562778947279400250L;
    }

    public record TransactionActivityModelRecord(
            String id,
             Date transactionDate,
            String instrumentId,
            String transactionName,
            @NumberFormat(pattern = "#.####")
            BigDecimal amount,
            String attributeId,
            int originalPeriodId,
            long instrumentAttributeVersionId,
            AccountingPeriod accountingPeriod,
            long batchId,
            Source source,
            String sourceId,
            Date postingDate,
            Integer effectiveDate,
            Map<String, Object> attributes
    ) implements Serializable {
        private static final long serialVersionUID = 8444760102552307163L;
    }

    public record InstrumentAttributeModelRecord (
            InstrumentAttributeVersionType type,
            String id,
            Date effectiveDate,
            String instrumentId,
            String attributeId,
            long batchId,
            Date endDate,
            int periodId,
            long versionId,
            long previousVersionId,
            Source source,
            String sourceId,
            Map<String, Object> attributes,
            Date postingDate) implements Serializable {
        private static final long serialVersionUID = 666959603291768207L;
    }

    public record InstrumentReplayRecord(String instrumentId, int postingDate, int effectiveDate) implements Serializable {
        private static final long serialVersionUID = 1132335290303185578L;

    }

    public record AttributeLevelLtdRecord(String metricName, String instrumentId, String attributeId, int postingDate, int accountingPeriod, BigDecimal amount) implements Serializable {
        private static final long serialVersionUID = 8657459429669577477L;

    }

    public record InstrumentLevelLtdRecord(String metricName, String instrumentId, int postingDate, int accountingPeriod, BigDecimal amount) implements Serializable {
        private static final long serialVersionUID = -1870745717812788971L;

    }

    public record MetricLevelLtdRecord(String metricName, int postingDate, int accountingPeriod, BigDecimal amount) implements Serializable {
        private static final long serialVersionUID = -8848774298173647510L;

    }

    public record TrendAnalysisRecord(String metricName, String[] accountingPeriods, BigDecimal[] endingBalances) implements Serializable {
        private static final long serialVersionUID = 9154995883257602702L;
    }

    public record RankedMetricRecord(int rank, String metricName, BigDecimal balance) implements Serializable {
        private static final long serialVersionUID = 605044639104245053L;
    }

    public record MonthOverMonthActivityRecord(Integer accountingPeriodId, String metricName, BigDecimal activityAmount) implements Serializable {
        private static final long serialVersionUID = -5096405985388460563L;
    }

    public record MonthOverMonthMetricActivityRecord(List<Map<String, String>> monthOverMonthSeries, List<Map<String, Object>> momData) implements Serializable {
        private static final long serialVersionUID = -4585296236786537726L;
    }

    public record FlatAttributeLevelLtdRecord(
            @Field("_id")
            String id,
            String metricName,
            String instrumentId,
            String attributeId,
            Integer accountingPeriodId,
            Integer postingDate,
            @Field("balance.activity") BigDecimal activity,
            @Field("balance.beginningBalance") BigDecimal beginningBalance,
            @Field("balance.endingBalance") BigDecimal endingBalance
    )  implements Serializable {
        private static final long serialVersionUID = -7010894295118952584L;
    }

    public record FlatInstrumentLevelLtdRecord(
            @Field("_id")
            String id,
            String metricName,
            String instrumentId,
            Integer accountingPeriodId,
            Integer postingDate,
            @Field("balance.activity") BigDecimal activity,
            @Field("balance.beginningBalance") BigDecimal beginningBalance,
            @Field("balance.endingBalance") BigDecimal endingBalance
    )  implements Serializable {
        private static final long serialVersionUID = -7010894295118952584L;
    }

    public record FlatMetricLevelLtdRecord(
            @Field("_id")
            String id,
            String metricName,
            Integer accountingPeriodId,
            Integer postingDate,
            @Field("balance.activity") BigDecimal activity,
            @Field("balance.beginningBalance") BigDecimal beginningBalance,
            @Field("balance.endingBalance") BigDecimal endingBalance
    )  implements Serializable {
        private static final long serialVersionUID = -2108325930955107947L;
    }
}