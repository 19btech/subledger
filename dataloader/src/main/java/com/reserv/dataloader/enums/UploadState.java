package com.reserv.dataloader.enums;

/**
 * Lifecycle of an asynchronous file-upload processing job, tracked per {@code uploadId}.
 * See docs/K8S_SCALING_STRATEGY.md (Stage 0 — async upload endpoint).
 */
public enum UploadState {
    PENDING,
    IN_PROGRESS,
    COMPLETED,
    FAILED
}
