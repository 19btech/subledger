package com.fyntrac.common.model;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.Event;
import com.fyntrac.common.entity.ExecutionState;
import lombok.Builder;
import lombok.Data;

import java.util.Date;
import java.util.List;
import java.util.Map;
import com.fyntrac.common.enums.ModelExecutionType;
import org.apache.poi.ss.usermodel.Workbook;

@Data
@Builder
public class ModelWorkflowContext {
    public List<Records.InstrumentAttributeModelRecord> currentInstrumentAttribute;
    public List<Records.InstrumentAttributeModelRecord> lastInstrumentAttribute;
    public List<Records.InstrumentAttributeModelRecord> firstInstrumentAttribute;
    private List<Map<String, Object>> iTransactions;
    private List<Map<String, Object>> iMetrics;
    private List<Map<String, Object>> iInstrumentAttributes;
    private List<Map<String, Object>> iExecutionDate;
    Records.ModelRecord excelModel;
    private ModelExecutionType executionType;
    private Date executionDate;
    private AccountingPeriod accountingPeriod;
    private ExecutionState executionState;
    private String instrumentId;
    private String tenantId;
    private int lastActivityPostingDate;
    private Workbook workbook;
    private List<Event> events;
}
