package com.fyntrac.common.dto.record;

import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.*;
import com.fyntrac.common.model.ModelWorkflowContext;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.format.annotation.NumberFormat;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.Size;
import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
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

    public record ModelExecutionMessageRecord(String tenantId, Integer executionDate, String key, boolean isLast)implements Serializable {
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

    public record InstrumentReplayRecord(String instrumentId,String attributeId, int postingDate, int effectiveDate) implements Serializable {
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

    public record DiagnosticReportRequestRecord(String tenant, String instrumentId, String modelId,
                                                String postingDate) implements Serializable {
        private static final long serialVersionUID = 4708400430638644109L;
    }

    public record DiagnosticReportModelDataRecord(ModelWorkflowContext excelData, File excelMode) implements Serializable {
        private static final long serialVersionUID = 8522699881876081280L;
    }

    public record DiagnosticReportDataRecord(Map<String, List<Map<String,
            Object>>> valueMapList) implements Serializable {
        private static final long serialVersionUID = -212786685516433649L;
    }

    public record DataFileRecord(String name, String id) implements Serializable {
        private static final long serialVersionUID = 2455439474427522772L;
    }

    public record InstrumentMessageRecord(String tenantId, String[] instrumentIds, String[] models) implements Serializable {
        private static final long serialVersionUID = 4754051711821283813L;
    }

    public record AuthticationRecord(String email, String pswd) implements Serializable {
        private static final long serialVersionUID = -1704199134687276951L;
    }

    public record UserTenantRecord(User user, List<Tenant> tenants) implements Serializable {
        private static final long serialVersionUID = -4812970440663173267L;
    }

    public record OptionRequest(
            @NotBlank(message = "Label is required")
            String label,

            @NotBlank(message = "Value is required")
            String value
    ) implements Serializable {private static final long serialVersionUID = 3111803440852648881L;}

    public record SourceMappingRequest(
            @NotNull(message = "Source table is required")
            String sourceTable,

            @NotNull(message = "Source columns are required")
            List<OptionRequest> sourceColumns,

            List<OptionRequest> versionType,

            List<OptionRequest> dataMapping,
            @NotNull(message = "Source columns are required")
            String fieldType
    ) implements Serializable {private static final long serialVersionUID = -6688173484561516379L;}

    public record TriggerSetupRequest(
            @NotNull(message = "Trigger type is required")
            String triggerType,

            String triggerCondition,

            List<OptionRequest> triggerSource
    ) implements Serializable {private static final long serialVersionUID = 5176050122493280517L;}

    public record EventConfigurationRequest(
            @NotBlank(message = "Event ID is required")
            String eventId,

            @NotBlank(message = "Event name is required")
            String eventName,

            @NotNull(message = "Priority is required")
            @Positive(message = "Priority must be positive")
            Integer priority,

            String description,

            @NotNull(message = "Trigger setup is required")
            TriggerSetupRequest triggerSetup,

            @NotNull(message = "Source mappings are required")
            List<SourceMappingRequest> sourceMappings
    ) implements Serializable {private static final long serialVersionUID = 3791186632733494104L;}

    public record OptionResponse(
            String label,
            String value
    ) implements Serializable {private static final long serialVersionUID = 1180567008956378960L;}

    public record SourceMappingResponse(
            String sourceTable,
            List<OptionResponse> sourceColumns,
            List<OptionResponse> versionType,
            List<OptionResponse> dataMapping,
            String fieldType
    ) implements Serializable {private static final long serialVersionUID = 6415178979282662812L;}

    public record TriggerSetupResponse(
            String triggerType,
            String triggerCondition,
            List<OptionResponse> triggerSource
    ) implements Serializable {private static final long serialVersionUID = -298061981443835860L;}

    public record EventConfigurationResponse(
            String id,
            String eventId,
            String eventName,
            Integer priority,
            String description,
            TriggerSetupResponse triggerSetup,
            List<SourceMappingResponse> sourceMappings,
            LocalDateTime createdAt,
            LocalDateTime updatedAt,
            String createdBy,
            String updatedBy,
            Boolean isActive,
            Boolean isDeleted
    ) implements Serializable {private static final long serialVersionUID = 9082175583455561988L;}

    public record EventConfigurationBasicResponse(
            String id,
            String eventId,
            String eventName,
            Integer priority,
            String description,
            LocalDateTime createdAt
    ) implements Serializable {private static final long serialVersionUID = 7199490834201351323L;}

    public record TransactionActivityAmountRecord(
            String transactionName,
            Integer effectiveDate,
            BigDecimal totalAmount
    ) implements Serializable {private static final long serialVersionUID = -7045460909449564112L;}

    public record ExcelModelEventKey(
            Integer postingDate,
            Integer effectiveDate,
            String instrumentId,
            String attributeId
    ) implements Serializable {private static final long serialVersionUID = 4292426438651236257L;}

    public record TransactionKeyRecord(String transactionName, Integer effectiveDate) implements Serializable {private static final long serialVersionUID = 3338343449065429379L;}

    public record CustomTableRequestRecord(
            @NotBlank(message = "Table name is required")
            String tableName,

            String description,

            @NotNull(message = "Table type is required")
            CustomTableType tableType,

            @NotNull(message = "Columns are required")
            @Size(min = 1, message = "At least one column is required")
            @Valid
            List<CustomTableColumn> columns,

            @NotNull(message = "Primary keys are required")
            @Size(min = 1, message = "At least one primary key must be selected")
            List<String> primaryKeys,

            String referenceColumn,

            String referenceTable
    ) implements Serializable {private static final long serialVersionUID = -2354948109672778094L;}

    public record CustromTableResponseRecord(
            String id,
            String tableName,
            String description,
            CustomTableType tableType,
            LocalDateTime createdAt,
            LocalDateTime updatedAt
    ) implements Serializable {private static final long serialVersionUID = 6620969286174533592L;}

    public record ApiResponseRecord<T>(
            boolean success,
            String message,
            T data,
            String error
    ) {
        public static <T> ApiResponseRecord<T> success(T data) {
            return new ApiResponseRecord<>(true, null, data, null);
        }

        public static <T> ApiResponseRecord<T> success(String message, T data) {
            return new ApiResponseRecord<>(true, message, data, null);
        }

        public static <T> ApiResponseRecord<T> error(String error) {
            return new ApiResponseRecord<>(false, null, null, error);
        }

        public static <T> ApiResponseRecord<T> error(String message, String error) {
            return new ApiResponseRecord<>(false, message, null, error);
        }
    }

    public record CustomTableColumnsRecord(String tableName, List<String> columns) {private static final long serialVersionUID = -7350503432076490961L;}
}