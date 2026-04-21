package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "EventHistoryBatchTracker")
public class EventHistoryBatchTracker {

    @Id
    private String id;

    @Indexed
    private Long jobId;

    @Indexed
    private String tenantId;

    private Integer expectedInstrumentCount;
    
    private Integer processedInstrumentCount;

    @Indexed
    private String status; // PRODUCED, PROCESSING, COMPLETED, FAILED

    private String errorMessage;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
