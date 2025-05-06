package com.fyntrac.common.dto.record;

import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.UploadStatus;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.stereotype.Component;

import java.io.Serializable;
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

    public static Records.CommonMessageRecord createCommonMessage(String tenant,Date executionDate, String key){
        return createRecord(() -> new Records.CommonMessageRecord(tenant,executionDate, key));
    }

    public static Records.ModelRecord createModelRecord(Model model, ModelFile modelFile){
        return createRecord(() -> new Records.ModelRecord(model, modelFile));
    }

    public static Records.ExcelModelRecord createExcelModelRecord(Model model, Workbook excelModel){
        return createRecord(() -> new Records.ExcelModelRecord(model, excelModel));
    }

    public static Records.MetricRecord createMetricRecord(String metricName, String instrumentid, String attributeId, int accountingPeriod, double beginningBalance, double activity, double endingBalance) {
        return createRecord(() -> new Records.MetricRecord(metricName, instrumentid, attributeId, accountingPeriod, beginningBalance, activity, endingBalance));
    }

    public static Records.MetricRecord createMetricRecord(AttributeLevelLtd ltd) {
        return createRecord(() -> new Records.MetricRecord(ltd.getMetricName()
                , ltd.getInstrumentId()
                , ltd.getAttributeId()
                , ltd.getAccountingPeriodId()
                , ltd.getBalance().getBeginningBalance()
                , ltd.getBalance().getActivity()
                , ltd.getBalance().getEndingBalance()));
    }

    public static Records.MetricRecord createMetricRecord(InstrumentLevelLtd ltd) {
        return createRecord(() -> new Records.MetricRecord(ltd.getMetricName()
                , ltd.getInstrumentId()
                , ""
                , ltd.getAccountingPeriodId()
                , ltd.getBalance().getBeginningBalance()
                , ltd.getBalance().getActivity()
                , ltd.getBalance().getEndingBalance()));
    }

    public static Records.MetricRecord createMetricRecord(MetricLevelLtd ltd) {
        return createRecord(() -> new Records.MetricRecord(ltd.getMetricName()
                , ""
                , ""
                , ltd.getAccountingPeriodId()
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

}
