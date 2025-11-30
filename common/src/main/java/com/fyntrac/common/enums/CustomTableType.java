package com.fyntrac.common.enums;

public enum CustomTableType {
    REFERENCE,  // For lookup data with reference columns
    OPERATIONAL // For transactional data that can link to reference tables
}