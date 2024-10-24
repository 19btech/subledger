package com.fyntrac.common.config;

import lombok.Builder;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

@Data
@Builder
public class ReferenceData implements Serializable {
    @Serial
    private static final long serialVersionUID = 5540471025675268288L;
    int currentAccountingPeriodId;
    int previoudAccountingPeriodId;
}
