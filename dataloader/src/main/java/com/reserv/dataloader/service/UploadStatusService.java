package com.reserv.dataloader.service;

import com.reserv.dataloader.entity.UploadStatus;
import com.reserv.dataloader.enums.UploadState;
import com.reserv.dataloader.repository.UploadStatusRepo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Backs the async upload status endpoint. See docs/K8S_SCALING_STRATEGY.md (Stage 0).
 */
@Service
@Slf4j
public class UploadStatusService {

    private final UploadStatusRepo uploadStatusRepo;

    @Autowired
    public UploadStatusService(UploadStatusRepo uploadStatusRepo) {
        this.uploadStatusRepo = uploadStatusRepo;
    }

    public UploadStatus markPending(long uploadId, boolean overwrite) {
        UploadStatus status = UploadStatus.builder()
                .uploadId(uploadId)
                .overwrite(overwrite)
                .state(UploadState.PENDING)
                .submittedAt(LocalDateTime.now())
                .build();
        return uploadStatusRepo.save(status);
    }

    public void markInProgress(long uploadId) {
        updateState(uploadId, UploadState.IN_PROGRESS, s -> s.setStartedAt(LocalDateTime.now()));
    }

    public void markCompleted(long uploadId) {
        updateState(uploadId, UploadState.COMPLETED, s -> s.setCompletedAt(LocalDateTime.now()));
    }

    public void markFailed(long uploadId, String errorMessage) {
        updateState(uploadId, UploadState.FAILED, s -> {
            s.setCompletedAt(LocalDateTime.now());
            s.setErrorMessage(errorMessage);
        });
    }

    public Optional<UploadStatus> getStatus(long uploadId) {
        return uploadStatusRepo.findByUploadId(uploadId);
    }

    private void updateState(long uploadId, UploadState state, java.util.function.Consumer<UploadStatus> mutator) {
        Optional<UploadStatus> existing = uploadStatusRepo.findByUploadId(uploadId);
        if (existing.isEmpty()) {
            log.warn("No UploadStatus found for uploadId={} while transitioning to {}", uploadId, state);
            return;
        }
        UploadStatus status = existing.get();
        status.setState(state);
        mutator.accept(status);
        uploadStatusRepo.save(status);
    }
}
