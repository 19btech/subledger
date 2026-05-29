package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "ExecutionInstance")
public class ExecutionInstance implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Id
    private String id; // This will be the correlationId for the entire run

    @Indexed
    private String tenantId;
    private Integer postingDate;
    private String modelType; // e.g., "DSL"

    private String status; // INITIALIZING, GENERATING_EVENTS, PROCESSING, AGGREGATING, GL_SYNC, COMPLETED, FAILED
    private Integer totalBatches;
    private Integer completedBatches;

    private Date startTime;
    private Date endTime;
    private String errorMessage;

    public boolean isComplete() {
        return totalBatches != null && completedBatches != null && totalBatches.equals(completedBatches);
    }
}
