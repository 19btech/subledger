package com.fyntrac.model.workflow;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.entity.InstrumentAttribute;
import lombok.Builder;
import lombok.Data;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class ModelWorkflowContext {
    private InstrumentAttribute currentInstrumentAttribute;
    private InstrumentAttribute lastInstrumentAttribute;
    private InstrumentAttribute firstInstrumentAttribute;
    private List<Map<String, Object>> iTransactions;
    private List<Map<String, Object>> iMetrics;
    private List<Map<String, Object>> iInstrumentAttributes;
    private List<Map<String, Object>> iExecutionDate;
    Records.ModelRecord excelModel;
    private ModelExecutionType executionType;
    private Date executionDate;
    private String instrumentId;
    private String attributeId;
    private AccountingPeriod accountingPeriod;
    private ExecutionState executionState;
}
