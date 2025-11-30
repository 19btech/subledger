package com.fyntrac.common.service;

import com.fyntrac.common.entity.CustomTableDefinition;

public interface CustomTableCreationStrategy {
    boolean supports(CustomTableDefinition tableDefinition);
    void createPhysicalTable(CustomTableDefinition tableDefinition);
    void dropPhysicalTable(String tableName);
    boolean tableExists(String tableName);
}
