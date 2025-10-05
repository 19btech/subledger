package com.fyntrac.common.dto.record;

import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.InstrumentAttributeVersionType;
import com.fyntrac.common.enums.Source;
import com.fyntrac.common.enums.TestStep;
import com.fyntrac.common.enums.UploadStatus;
import com.fyntrac.common.model.ModelWorkflowContext;
import com.fyntrac.common.utils.DateUtil;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.format.annotation.NumberFormat;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.text.ParseException;
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

    public static Records.GeneralLedgerMessageRecord createGeneralLedgerMessageRecord(String tenantId, long jobId){
        return createRecord(()->new Records.GeneralLedgerMessageRecord(tenantId, jobId));
    }

    public static Records.TransactionActivityRecord createTransactionActivityRecord(TransactionActivity transactionActivity, String tenantId){
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

    public static Records.ModelExecutionMessageRecord createModelExecutionMessage(String tenant,Integer executionDate, String key){
        return createRecord(() -> new Records.ModelExecutionMessageRecord(tenant,executionDate, key));
    }

    public static Records.ModelRecord createModelRecord(Model model, ModelFile modelFile){
        return createRecord(() -> new Records.ModelRecord(model, modelFile));
    }

    public static Records.ExcelModelRecord createExcelModelRecord(Model model, Workbook excelModel){
        return createRecord(() -> new Records.ExcelModelRecord(model, excelModel));
    }

    public static Records.MetricRecord createMetricRecord(String metricName, String instrumentid, String attributeId, int accountingPeriod,int postingDate, BigDecimal beginningBalance, BigDecimal activity, BigDecimal endingBalance) {
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

    public static Records.DocumentAttribute createDocumentAttribute(String attributeName, String attributeAlias,  String dataType) {
        return createRecord(() -> new Records.DocumentAttribute(attributeName, attributeAlias, dataType));
    }

    public static Records.QueryCriteriaItem createQueryCriteriaItem(String attributeName, String operator,String values, List<String> filters, String logicalOperator) {
        return createRecord(() -> new Records.QueryCriteriaItem(attributeName, operator, values, filters, logicalOperator));
    }

    public static Records.TransactionActivityReplayRecord createTransactionActivityReplayRecord(String instrumentId
    , String attributeId
    , Integer replayDate) {
     return createRecord(() -> new Records.TransactionActivityReplayRecord(instrumentId, attributeId, replayDate));
    }

    public  static Records.ExecutionDateRecord createExcutionDateRecord(Date executionDate, Date	lastExecutionDate, Date	replayDate) {
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
        , batchId, source, sourceId, DateUtil.convertToDateFromYYYYMMDD(postingDate),effectiveDate, attributes));
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
                        ,activity.getEffectiveDate()
                        , activity.getAttributes());
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static Records.InstrumentAttributeModelRecord createInstrumentAttributeModelRecord( InstrumentAttributeVersionType type,
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

    public static Records.InstrumentAttributeModelRecord createInstrumentAttributeModelRecord( InstrumentAttributeVersionType type,
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

    public static Records.InstrumentAttributeRecord createInstrumentAttributeRecord( Date effectiveDate
                                                                              ,String instrumentId
                                                                              ,String attributeId
                                                                              ,Date endDate
                                                                              ,int periodId
                                                                              ,long versionId
                                                                              ,Map<String, Object> attributes
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
        return createRecord(() -> new Records.AttributeLevelLtdRecord(metricName, instrumentId,attributeId, postingDate, accountingPeriod, amount));

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
        return createRecord(()-> new Records.RankedMetricRecord(rank,metricName,balance));
    }

    public static Records.MonthOverMonthActivityRecord createMonthOverMonthActivityRecord(Integer accountingPeriodId, String metricName, BigDecimal activityAmount) {
        return createRecord(()-> new Records.MonthOverMonthActivityRecord(accountingPeriodId, metricName, activityAmount));
    }

    public static Records.MonthOverMonthMetricActivityRecord createMonthOverMonthMetricActivityRecord(List<Map<String, String>> monthOverMonthSeries, List<Map<String, Object>> momData) {
        return createRecord(()-> new Records.MonthOverMonthMetricActivityRecord(monthOverMonthSeries, momData));
    }

    public static Records.DiagnosticReportRequestRecord createRiagnosticReportRequestRecord(String tenant, String instrumentId, String modelId) {
        return createRecord(()-> new Records.DiagnosticReportRequestRecord(tenant, instrumentId,modelId));
    }

    public static Records.DiagnosticReportModelDataRecord createDiagnosticReportModelDataRecord(ModelWorkflowContext excelData, File excelMode) {
        return createRecord(()-> new Records.DiagnosticReportModelDataRecord(excelData, excelMode));
    }

    public static Records.DiagnosticReportDataRecord createDiagnosticReportDataRecord(List<Records.DocumentAttribute> transactionActivityHeader,
                                             List<Map<String, Object>> transactionActivityData,
                                             List<Records.DocumentAttribute> instrumentAttributeHeader,
                                             List<Map<String, Object>> instrumentAttributeData,
                                             List<Records.DocumentAttribute> balancesHeader,
                                             List<Map<String, Object>> balancesData,
                                             List<Records.DocumentAttribute> executionStateHeader,
                                             List<Map<String, Object>> executionStateData) {
        return createRecord(()-> new Records.DiagnosticReportDataRecord(transactionActivityHeader,
                transactionActivityData,
                instrumentAttributeHeader,
                instrumentAttributeData,
                balancesHeader,
                balancesData,
                executionStateHeader,
                executionStateData));
    }

    public static Records.DataFileRecord createDataFileRecord(String name, String id) {
        return createRecord(()-> new Records.DataFileRecord(name, id));
    }

    public static Records.InstrumentMessageRecord CreateInstrumentMessageRecord(String tenantId,
                                                                                String[] instrumentIds,
                                                                                String[] models) {
        return createRecord(() -> new Records.InstrumentMessageRecord(tenantId, instrumentIds, models));
    }
}
