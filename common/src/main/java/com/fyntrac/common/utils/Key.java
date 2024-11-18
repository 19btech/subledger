package com.fyntrac.common.utils;

import com.fyntrac.common.entity.InstrumentAttribute;

public class Key {

    public static String accountingPeriodKey(String tenantId) {
        return tenantId + "-" + "AP";
    }

    public static String previoudPeriodInstrumentsKey(String tenantId){
        return tenantId + "PERVIOUD_PERIOD_INSTRUMENTS";
    }

    public static String allAttributeLevelLtdKeyList(String tenantId) {
        return tenantId + "ATTRIBUTE_LEVEL_LTD_KEY_LIST";
    }

    public static String allInstrumentLevelLtdKeyList(String tenantId) {
        return tenantId + "INSTRUMENT_LEVEL_LTD_KEY_LIST";
    }

    public static String allMetricLevelLtdKeyList(String tenantId) {
        return tenantId + "METRIC_LEVEL_LTD_KEY_LIST";
    }

    public static String allSubledgerMappingList(String tenantId){ return tenantId + "SUBLEDGER_MAPPING";}
    public static String allAccountTypeList(String tenantId){ return tenantId + "ACCOUNT_TYPE";}

    public static String allChartOfAccountList(String tenantId){ return tenantId + "CHART_OF_ACCOUNT";}
    public static String allMetricList(String tenantId){ return tenantId + "METRIC_NAME";}
    public static String reclassMessageList(String tenantId, long runId) {
        return tenantId + "RECLASS_MESSAGE" + "-" + runId;
    }

    public static String reclassAttributes(String tenantId) {
        return tenantId + "RECLASS_ATTRIBUTES";
    }

    public static String instrumentAttributeList(String tenantId) {
        return tenantId + "INSTRUMENT_ATTRIBUTE";
    }

    public static String instrumentAttributeKey(String tenantId
            , String attributeId, String instrumentId, int periodId) {
        return tenantId + attributeId + instrumentId + periodId;
    }

    public static String aggregationKey() {
        return "AGG-KEY";
    }
}
