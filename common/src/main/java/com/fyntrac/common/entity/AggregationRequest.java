package com.fyntrac.common.entity;

import com.fyntrac.common.enums.AggregationRequestType;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serial;
import java.io.Serializable;

@Slf4j
@Data
@Builder
public class AggregationRequest implements Serializable {
    @Serial
    private static final long serialVersionUID = 6501892690932571060L;
    private String key;
    private boolean isAggregationComplete;
    private boolean isInprogress;
    private String tenantId;
    private AggregationRequestType requestType;
    private int postingDate;
}
