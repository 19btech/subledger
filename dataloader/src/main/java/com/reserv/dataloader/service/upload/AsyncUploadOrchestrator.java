package com.reserv.dataloader.service.upload;

import com.fyntrac.common.config.TenantContextHolder;
import com.reserv.dataloader.service.UploadStatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * Background dispatch for the async upload endpoints (Stage 0, docs/K8S_SCALING_STRATEGY.md).
 *
 * <p>Deliberately its own bean, separate from {@link FileUploadService}: {@code @Async} only
 * intercepts calls that arrive through the Spring proxy from a <em>different</em> bean — a
 * same-class ("self") call bypasses the proxy and would run synchronously. Callers (controllers)
 * invoke {@link #processAsync} directly, satisfying that requirement.</p>
 *
 * <p>{@link TenantContextHolder} is a plain {@code ThreadLocal} tied to the calling thread — it
 * does not propagate onto the async executor's pooled threads on its own, so the caller must
 * capture the tenant on the request thread and pass it in explicitly here.</p>
 */
@Service
@Slf4j
public class AsyncUploadOrchestrator {

    private final FileUploadService fileUploadService;
    private final UploadStatusService uploadStatusService;

    @Autowired
    public AsyncUploadOrchestrator(FileUploadService fileUploadService, UploadStatusService uploadStatusService) {
        this.fileUploadService = fileUploadService;
        this.uploadStatusService = uploadStatusService;
    }

    @Async("uploadTaskExecutor")
    public void processAsync(long uploadId, boolean isOverwrite, Set<String> validFileSet, String tenant) {
        TenantContextHolder.setTenant(tenant);
        uploadStatusService.markInProgress(uploadId);
        try {
            fileUploadService.processFileUploadPipeline(isOverwrite, uploadId, validFileSet);
            uploadStatusService.markCompleted(uploadId);
        } catch (Throwable t) {
            String stackTrace = com.fyntrac.common.utils.StringUtil.getStackTrace(t);
            log.error("Async upload processing failed for uploadId={}: {}", uploadId, stackTrace);
            uploadStatusService.markFailed(uploadId, t.getMessage());
        } finally {
            // Pool threads are reused across tenants — clear so a later task on this same thread
            // never inherits a stale tenant.
            TenantContextHolder.clear();
        }
    }
}
