package com.fyntrac.common.utils;

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
}
