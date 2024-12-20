package com.fyntrac.common.entity;

import com.fyntrac.common.enums.AggregationLevel;
import com.fyntrac.common.enums.AttributeVersion;
import lombok.Builder;
import lombok.Data;

@Data
public class ModelConfig {
    String[] transactions;
    String[] metrics;
    AggregationLevel aggregationLevel;
    AttributeVersion[] attributeVersion;
}
