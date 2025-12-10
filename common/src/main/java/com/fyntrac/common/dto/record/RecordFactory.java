package com.fyntrac.common.dto.record;

import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.*;
import com.fyntrac.common.model.ModelWorkflowContext;
import com.fyntrac.common.utils.DateUtil;
import org.apache.poi.ss.formula.functions.T;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.format.annotation.NumberFormat;
import org.springframework.stereotype.Component;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.Size;
import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.List;
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
        if (accountingPeriod == null) {
            return createRecord(() -> new Records.AccountingPeriodRecord(
                    0
                    , "_ _ / _ _"
                    , 0
                    , 0
                    , 0));
        } else {
            return createRecord(() -> new Records.AccountingPeriodRecord(
                    accountingPeriod.getPeriodId()
                    , accountingPeriod.getPeriod()
                    , accountingPeriod.getFiscalPeriod()
                    , accountingPeriod.getYear()
                    , accountingPeriod.getStatus()));
        }
    }

    public static Records.GeneralLedgerMessageRecord createGeneralLedgerMessageRecord(String tenantId, long jobId) {
        return createRecord(() -> new Records.GeneralLedgerMessageRecord(tenantId, jobId));
    }

    public static Records.TransactionActivityRecord createTransactionActivityRecord(TransactionActivity transactionActivity, String tenantId) {
        return createRecord(() -> {
            try {
                return new Records.TransactionActivityRecord(
                        tenantId,
                        transactionActivity.getId(),
                        transactionActivity.getTransactionDate(),
                        transactionActivity.getInstrumentId(),
                        transactionActivity.getTransactionName(),
                        transactionActivity.getAmount(),
                        transactionActivity.getAttributeId(),
                        transactionActivity.getPeriodId(),
                        transactionActivity.getOriginalPeriodId());
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static Records.InstrumentAttributeRecord createInstrumentAttributeRecord(InstrumentAttribute instrumentAttribute) {
        return createRecord(() -> new Records.InstrumentAttributeRecord(instrumentAttribute.getEffectiveDate()
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
        return createRecord(() -> new Records.ReclassMessageRecord(tenantId, dataKey));
    }

    public static Records.AccountingPeriodCloseMessageRecord createAccountingPeriodCloseMessage(String tenant, Collection<Batch> batches) {
        return createRecord(() -> new Records.AccountingPeriodCloseMessageRecord(tenant, batches));
    }

    public static Records.ModelExecutionMessageRecord createModelExecutionMessage(String tenant, Integer executionDate, String key) {
        return createRecord(() -> new Records.ModelExecutionMessageRecord(tenant, executionDate, key));
    }

    public static Records.ModelRecord createModelRecord(Model model, ModelFile modelFile) {
        return createRecord(() -> new Records.ModelRecord(model, modelFile));
    }

    public static Records.ExcelModelRecord createExcelModelRecord(Model model, Workbook excelModel) {
        return createRecord(() -> new Records.ExcelModelRecord(model, excelModel));
    }

    public static Records.MetricRecord createMetricRecord(String metricName, String instrumentid, String attributeId, int accountingPeriod, int postingDate, BigDecimal beginningBalance, BigDecimal activity, BigDecimal endingBalance) {
        return createRecord(() -> new Records.MetricRecord(metricName
                , instrumentid
                , attributeId
                , accountingPeriod
                , DateUtil.convertToDateFromYYYYMMDD(postingDate)
                , beginningBalance
                , activity
                , endingBalance));
    }

    public static Records.MetricRecord createMetricRecord(AttributeLevelLtd ltd) {
        return createRecord(() -> new Records.MetricRecord(ltd.getMetricName()
                , ltd.getInstrumentId()
                , ltd.getAttributeId()
                , ltd.getAccountingPeriodId()
                , DateUtil.convertToDateFromYYYYMMDD(ltd.getPostingDate())
                , ltd.getBalance().getBeginningBalance()
                , ltd.getBalance().getActivity()
                , ltd.getBalance().getEndingBalance()));
    }

    public static Records.MetricRecord createMetricRecord(InstrumentLevelLtd ltd) {
        return createRecord(() -> new Records.MetricRecord(ltd.getMetricName()
                , ltd.getInstrumentId()
                , ""
                , ltd.getAccountingPeriodId()
                , DateUtil.convertToDateFromYYYYMMDD(ltd.getPostingDate())
                , ltd.getBalance().getBeginningBalance()
                , ltd.getBalance().getActivity()
                , ltd.getBalance().getEndingBalance()));
    }

    public static Records.MetricRecord createMetricRecord(MetricLevelLtd ltd) {
        return createRecord(() -> new Records.MetricRecord(ltd.getMetricName()
                , ""
                , ""
                , ltd.getAccountingPeriodId()
                , DateUtil.convertToDateFromYYYYMMDD(ltd.getPostingDate())
                , ltd.getBalance().getBeginningBalance()
                , ltd.getBalance().getActivity()
                , ltd.getBalance().getEndingBalance()));
    }

    public static Records.DateRequestRecord createDateRequest(String date) {
        return createRecord(() -> new Records.DateRequestRecord(date));
    }

    public static Records.ModelOutputData createModelOutputData(List<Map<String, Object>> transactionActivityList, List<Map<String, Object>> instrumentAttributeList) {
        return createRecord(() -> new Records.ModelOutputData(transactionActivityList, instrumentAttributeList));
    }

    public static Records.DocumentAttribute createDocumentAttribute(String attributeName, String attributeAlias, String dataType) {
        return createRecord(() -> new Records.DocumentAttribute(attributeName, attributeAlias, dataType));
    }

    public static Records.QueryCriteriaItem createQueryCriteriaItem(String attributeName, String operator, String values, List<String> filters, String logicalOperator) {
        return createRecord(() -> new Records.QueryCriteriaItem(attributeName, operator, values, filters, logicalOperator));
    }

    public static Records.TransactionActivityReplayRecord createTransactionActivityReplayRecord(String instrumentId
            , String attributeId
            , Integer replayDate) {
        return createRecord(() -> new Records.TransactionActivityReplayRecord(instrumentId, attributeId, replayDate));
    }

    public static Records.ExecutionDateRecord createExcutionDateRecord(Date executionDate, Date lastExecutionDate, Date replayDate) {
        return createRecord(() -> new Records.ExecutionDateRecord(executionDate, lastExecutionDate, replayDate));
    }

    public static Records.ExecuteAggregationMessageRecord createExecutionAggregationRecord(String tenantId, long jobId, Long aggregationDate) {
        return createRecord(() -> new Records.ExecuteAggregationMessageRecord(tenantId, jobId, aggregationDate));
    }

    public static Records.ExcelTestStepRecord createExcelTestStepRecord(TestStep step, String type, String input) {
        return createRecord(() -> new Records.ExcelTestStepRecord(step, type, input));
    }

    public static Records.MetricAttributeRow createMetricAttributeRow(int rowNumber, String metricName, String attributeId) {
        return createRecord(() -> new Records.MetricAttributeRow(rowNumber, metricName, attributeId));
    }

    public static Records.TransactionAttributeRow createTransactionAttributeRow(int rowNumber, String TransactionName, String attributeId) {
        return createRecord(() -> new Records.TransactionAttributeRow(rowNumber, TransactionName, attributeId));
    }

    public static Records.TransactionActivityModelRecord createTransactionActivityModelRecord(
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
            Integer postingDate,
            Integer effectiveDate,
            Map<String, Object> attributes) {
        return createRecord(() -> new Records.TransactionActivityModelRecord(id
                , transactionDate
                , instrumentId
                , transactionName
                , amount
                , attributeId
                , originalPeriodId
                , instrumentAttributeVersionId
                , accountingPeriod
                , batchId, source, sourceId, DateUtil.convertToDateFromYYYYMMDD(postingDate), effectiveDate, attributes));
    }

    public static Records.TransactionActivityModelRecord createTransactionActivityModelRecord(TransactionActivity activity) {
        return createRecord(() -> {
            try {
                return new Records.TransactionActivityModelRecord(activity.getId()
                        , activity.getTransactionDate()
                        , activity.getInstrumentId()
                        , activity.getTransactionName()
                        , activity.getAmount()
                        , activity.getAttributeId()
                        , activity.getOriginalPeriodId()
                        , activity.getInstrumentAttributeVersionId()
                        , activity.getAccountingPeriod()
                        , activity.getBatchId()
                        , activity.getSource()
                        , activity.getSourceId()
                        , DateUtil.convertToDateFromYYYYMMDD(activity.getPostingDate())
                        , activity.getEffectiveDate()
                        , activity.getAttributes());
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static Records.InstrumentAttributeModelRecord createInstrumentAttributeModelRecord(InstrumentAttributeVersionType type,
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
                                                                                              Integer postingDate) {
        return createRecord(() -> new Records.InstrumentAttributeModelRecord(
                type,
                id,
                effectiveDate,
                instrumentId,
                attributeId,
                batchId,
                endDate,
                periodId,
                versionId,
                previousVersionId,
                source,
                sourceId,
                attributes, DateUtil.convertToDateFromYYYYMMDD(postingDate)));

    }

    public static Records.InstrumentAttributeModelRecord createInstrumentAttributeModelRecord(InstrumentAttributeVersionType type,
                                                                                              InstrumentAttribute instrumentAttribute) {
        return createRecord(() -> new Records.InstrumentAttributeModelRecord(
                type,
                instrumentAttribute.getId(),
                instrumentAttribute.getEffectiveDate(),
                instrumentAttribute.getInstrumentId(),
                instrumentAttribute.getAttributeId(),
                instrumentAttribute.getBatchId(),
                instrumentAttribute.getEndDate(),
                instrumentAttribute.getPeriodId(),
                instrumentAttribute.getVersionId(),
                instrumentAttribute.getPreviousVersionId(),
                instrumentAttribute.getSource(),
                instrumentAttribute.getSourceId(),
                instrumentAttribute.getAttributes(), DateUtil.convertToDateFromYYYYMMDD(instrumentAttribute.getPostingDate())));

    }

    public static Records.InstrumentAttributeRecord createInstrumentAttributeRecord(Date effectiveDate
            , String instrumentId
            , String attributeId
            , Date endDate
            , int periodId
            , long versionId
            , Map<String, Object> attributes
            , AccountingPeriod accountingPeriod) {

        return createRecord(() -> new Records.InstrumentAttributeRecord(
                effectiveDate,
                instrumentId,
                attributeId,
                endDate,
                periodId,
                versionId,
                attributes));

    }

    public static Records.InstrumentReplayRecord createInstrumentReplayRecord(String instrumentId, int postingDate, int effectiveDate) {
        return createRecord(() -> new Records.InstrumentReplayRecord(instrumentId, postingDate, effectiveDate));
    }

    public static Records.AttributeLevelLtdRecord createAttributeLevelLtdRecord(String metricName, String instrumentId, String attributeId, int postingDate, int accountingPeriod, BigDecimal amount) {
        return createRecord(() -> new Records.AttributeLevelLtdRecord(metricName, instrumentId, attributeId, postingDate, accountingPeriod, amount));

    }

    public static Records.InstrumentLevelLtdRecord createInstrumentLevelLtdRecord(String metricName, String instrumentId, int postingDate, int accountingPeriod, BigDecimal amount) {
        return createRecord(() -> new Records.InstrumentLevelLtdRecord(metricName, instrumentId, postingDate, accountingPeriod, amount));

    }

    public static Records.MetricLevelLtdRecord createMetricLevelLtdRecord(String metricName, int postingDate, int accountingPeriod, BigDecimal amount) {
        return createRecord(() -> new Records.MetricLevelLtdRecord(metricName, postingDate, accountingPeriod, amount));

    }

    public static Records.TrendAnalysisRecord createTrendAnalysisRecord(String metricName, String[] accountingPeriods, BigDecimal[] endingBalances) {
        return createRecord(() -> new Records.TrendAnalysisRecord(metricName, accountingPeriods, endingBalances));
    }

    public static Records.RankedMetricRecord createRankedMetricRecord(int rank, String metricName, BigDecimal balance) {
        return createRecord(() -> new Records.RankedMetricRecord(rank, metricName, balance));
    }

    public static Records.MonthOverMonthActivityRecord createMonthOverMonthActivityRecord(Integer accountingPeriodId, String metricName, BigDecimal activityAmount) {
        return createRecord(() -> new Records.MonthOverMonthActivityRecord(accountingPeriodId, metricName, activityAmount));
    }

    public static Records.MonthOverMonthMetricActivityRecord createMonthOverMonthMetricActivityRecord(List<Map<String, String>> monthOverMonthSeries, List<Map<String, Object>> momData) {
        return createRecord(() -> new Records.MonthOverMonthMetricActivityRecord(monthOverMonthSeries, momData));
    }

    public static Records.DiagnosticReportRequestRecord createRiagnosticReportRequestRecord(String tenant,
                                                                                            String instrumentId,
                                                                                            String modelId,
                                                                                            String postingDate) {
        return createRecord(() -> new Records.DiagnosticReportRequestRecord(tenant, instrumentId, modelId, postingDate));
    }

    public static Records.DiagnosticReportModelDataRecord createDiagnosticReportModelDataRecord(ModelWorkflowContext excelData, File excelMode) {
        return createRecord(() -> new Records.DiagnosticReportModelDataRecord(excelData, excelMode));
    }

    public static Records.DiagnosticReportDataRecord createDiagnosticReportDataRecord(Map<String, List<Map<String,
            Object>>> valueMapList) {
        return createRecord(() -> new Records.DiagnosticReportDataRecord(valueMapList));
    }

    public static Records.DataFileRecord createDataFileRecord(String name, String id) {
        return createRecord(() -> new Records.DataFileRecord(name, id));
    }

    public static Records.InstrumentMessageRecord CreateInstrumentMessageRecord(String tenantId,
                                                                                String[] instrumentIds,
                                                                                String[] models) {
        return createRecord(() -> new Records.InstrumentMessageRecord(tenantId, instrumentIds, models));
    }

    public static Records.AuthticationRecord createAuthenticationRecord(String email, String pswd) {
        return createRecord(() -> new Records.AuthticationRecord(email, pswd));
    }

    public static Records.UserTenantRecord createUserTenantRecord(User user, List<Tenant> tenants) {
        return createRecord(() -> new Records.UserTenantRecord(user, tenants));
    }

    public static Records.OptionRequest createOptionRequestRecord(
            @NotBlank(message = "Label is required")
            String label,

            @NotBlank(message = "Value is required")
            String value
    ) {
        return createRecord(() -> new Records.OptionRequest(label, value));
    }

    public static Records.SourceMappingRequest createSourceMappingRequestRecord(
            @NotNull(message = "Source table is required")
            String sourceTable,

            @NotNull(message = "Source columns are required")
            List<Records.OptionRequest> sourceColumns,

            List<Records.OptionRequest> versionType,

            List<Records.OptionRequest> dataMapping,
            String fieldType
    ) {
        return createRecord(() -> new Records.SourceMappingRequest(sourceTable, sourceColumns, versionType,
                dataMapping, fieldType));
    }

    public static Records.TriggerSetupRequest createTriggerSetupRequestRecord(
            @NotNull(message = "Trigger type is required")
            String triggerType,

            String triggerCondition,

            List<Records.OptionRequest> triggerSource
    ) {
        return createRecord(() -> new Records.TriggerSetupRequest(triggerType, triggerCondition, triggerSource));
    }

    public static Records.EventConfigurationRequest createEventConfigurationRequestRecord(
            @NotBlank(message = "Event ID is required")
            String eventId,

            @NotBlank(message = "Event name is required")
            String eventName,

            @NotNull(message = "Priority is required")
            @Positive(message = "Priority must be positive")
            Integer priority,

            String description,

            @NotNull(message = "Trigger setup is required")
            Records.TriggerSetupRequest triggerSetup,

            @NotNull(message = "Source mappings are required")
            List<Records.SourceMappingRequest> sourceMappings
    ) {
        return createRecord(() -> new Records.EventConfigurationRequest(eventId, eventName, priority, description,
                triggerSetup, sourceMappings));
    }

    public static Records.OptionResponse createOptionResponseRecord(
            String label,
            String value
    ) {
        return createRecord(() -> new Records.OptionResponse(label, value));
    }

    public static Records.SourceMappingResponse createSourceMappingResponseRecord(
            String sourceTable,
            List<Records.OptionResponse> sourceColumns,
            List<Records.OptionResponse> versionType,
            List<Records.OptionResponse> dataMapping,
            String fieldType
    ) {
        return createRecord(() -> new Records.SourceMappingResponse(sourceTable, sourceColumns, versionType,
                dataMapping, fieldType));
    }

    public static Records.TriggerSetupResponse createTriggerSetupResponseRecord(
            String triggerType,
            String triggerCondition,
            List<Records.OptionResponse> triggerSource
    ) {
        return createRecord(() -> new Records.TriggerSetupResponse(triggerType, triggerCondition, triggerSource));
    }

    public static Records.EventConfigurationResponse createEventConfigurationResponseRecord(
            String id,
            String eventId,
            String eventName,
            Integer priority,
            String description,
            Records.TriggerSetupResponse triggerSetup,
            List<Records.SourceMappingResponse> sourceMappings,
            LocalDateTime createdAt,
            LocalDateTime updatedAt,
            String createdBy,
            String updatedBy,
            Boolean isActive,
            Boolean isDeleted
    ) {
        return createRecord(() -> new Records.EventConfigurationResponse(id, eventId, eventName, priority,
                description, triggerSetup, sourceMappings, createdAt, updatedAt, createdBy, updatedBy, isActive,
                isDeleted));
    }

    public static Records.EventConfigurationBasicResponse createEventConfigurationBasicResponseRecord(
            String id,
            String eventId,
            String eventName,
            Integer priority,
            String description,
            LocalDateTime createdAt
    ) {
        return createRecord(() -> new Records.EventConfigurationBasicResponse(id, eventId, eventName, priority,
                description, createdAt));
    }

    public static Records.ExcelModelEventKey createExcelModelEventKeyRecord(
            Integer postingDate,
            Integer effectiveDate,
            String instrumentId,
            String attributeId
    ) {
        return createRecord(() -> new Records.ExcelModelEventKey(postingDate, effectiveDate, instrumentId, attributeId));
    }

    public static Records.TransactionKeyRecord createTransactionKeyRecord(String transactionName,
                                                                          Integer effectiveDate) {
        return createRecord(() -> new Records.TransactionKeyRecord(transactionName, effectiveDate));
    }

    public static Records.TransactionActivityAmountRecord createTransactionActivityAmountRecord(String transactionName,
                                                                                                Integer effectiveDate,
                                                                                                BigDecimal totalAmount) {
        return createRecord(() -> new Records.TransactionActivityAmountRecord(transactionName, effectiveDate, totalAmount));
    }

    public static Records.CustomTableRequestRecord createCreateCustomTableRequestRecord(String tableName,
                                                                                        String description, CustomTableType tableType,
                                                                                        List<CustomTableColumn> columns,
                                                                                        List<String> primaryKeys,
                                                                                        String referenceColumn,
                                                                                        String referenceTable) {
        return createRecord(() -> new Records.CustomTableRequestRecord(tableName, description, tableType,
                columns, primaryKeys, referenceColumn, referenceTable));
    }

    public static Records.CustromTableResponseRecord createCustromTableResponseRecord(String id,
                                                                                      String tableName,
                                                                                      String description,
                                                                                      CustomTableType tableType,
                                                                                      LocalDateTime createdAt,
                                                                                      LocalDateTime updatedAt) {
        return createRecord(() -> new Records.CustromTableResponseRecord(id, tableName, description, tableType,
                createdAt, updatedAt));
    }

    public static Records.ApiResponseRecord createApiResponseRecord(boolean success,
    String message,
    T data,
    String error) {
        return createRecord(() -> new Records.ApiResponseRecord(success, message, data, error));
    }

    public static Records.CustomTableColumnsRecord creatCustomTableColumnsRecord(String tableName,
                                                                                 List<String> columns) {
        return createRecord(() -> new Records.CustomTableColumnsRecord(tableName, columns));
    }
}

