package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "ModelExecutionBatchLog")
@CompoundIndex(name = "tenant_date_logtype_idx", def = "{'tenantId': 1, 'postingDate': 1, 'logType': 1}")
public class ModelExecutionBatchLog implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Id
    private String id;
    private String jobId;
    private Integer batchNumber;   // page/chunk number from the producer side
    private String tenantId;
    private Integer postingDate;
    private String modelType;      // "PYTHON" | "EXCEL"
    private String logType;        // "EXECUTION_BATCH"
    private List<String> instrumentIds;
    private Integer instrumentCount;
    private Integer successCount;
    private Integer failedCount;
    private String status;         // "SUCCESS" | "PARTIAL_SUCCESS" | "FAILED"
    private String errorMessage;
    private Long durationMs;
    private Date createdAt;
}
