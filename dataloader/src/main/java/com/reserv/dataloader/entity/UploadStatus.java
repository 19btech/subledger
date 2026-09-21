package com.reserv.dataloader.entity;

import com.reserv.dataloader.enums.UploadState;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

/**
 * Tracks the lifecycle of an asynchronous file-upload processing job so a client can poll
 * {@code GET /api/dataloader/upload/status/{uploadId}} instead of holding an HTTP connection open
 * for the whole synchronous pipeline. See docs/K8S_SCALING_STRATEGY.md (Stage 0).
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "UploadStatus")
public class UploadStatus {

    @Id
    private String id;

    @Indexed(unique = true)
    private long uploadId;

    private UploadState state;
    private boolean overwrite;
    private LocalDateTime submittedAt;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;
    private String errorMessage;
}
