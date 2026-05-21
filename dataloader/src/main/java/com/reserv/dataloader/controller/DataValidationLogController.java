package com.reserv.dataloader.controller;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.ActivityDataValidationLog;
import com.fyntrac.common.entity.RefDataValidationLog;
import com.fyntrac.common.enums.ValidationType;
import com.fyntrac.common.repository.ActivityDataValidationLogRepository;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;

import java.util.List;

/**
 * REST API for querying reference-data and activity validation logs.
 *
 * <p>Base path: {@code /api/dataloader/validation-logs}
 *
 * <p>All endpoints are tenant-scoped via {@code TenantContextHolder} (populated
 * automatically by the request filter from the incoming JWT / tenant header).
 *
 * <h3>RefDataValidationLog (ACCOUNTING_RULES / JOURNAL_MAPPING)</h3>
 * <pre>
 *   GET /ref                                              → all
 *   GET /ref?jobId={id}                                   → by job
 *   GET /ref?jobId={id}&validationType=ACCOUNTING_RULES   → job + type
 *   GET /ref?jobId={id}&sourceTable=Aggregation           → job + table
 *   GET /ref?jobId={id}&errorCode=ERR_REQ_01              → job + error
 *   GET /ref/{id}                                         → by MongoDB id
 *   GET /ref/by-type/{validationType}                     → by type (all jobs)
 *   GET /ref/by-type/{validationType}/job/{jobId}         → by type + job
 * </pre>
 *
 * <h3>ActivityDataValidationLog (ACTIVITY / CUSTOM_ACTIVITY)</h3>
 * <pre>
 *   GET /activity                                          → all
 *   GET /activity?jobId={id}                              → by job
 *   GET /activity?jobId={id}&validationType=ACTIVITY      → job + type
 *   GET /activity?jobId={id}&errorCode=ERR_REF_03         → job + error
 *   GET /activity?jobId={id}&instrumentId=INST-001        → job + instrument
 *   GET /activity/{id}                                    → by MongoDB id
 *   GET /activity/by-type/{validationType}                → by type (all jobs)
 *   GET /activity/by-type/{validationType}/job/{jobId}    → by type + job
 * </pre>
 *
 * <h3>Combined</h3>
 * <pre>
 *   GET /summary?jobId={id}  → error counts across both collections
 * </pre>
 */
@RestController
@RequestMapping("/api/dataloader/validation-logs")
@Slf4j
public class DataValidationLogController {

    private final RefDataValidationLogRepository refLogRepository;
    private final ActivityDataValidationLogRepository activityLogRepository;

    public DataValidationLogController(
            RefDataValidationLogRepository refLogRepository,
            ActivityDataValidationLogRepository activityLogRepository) {
        this.refLogRepository = refLogRepository;
        this.activityLogRepository = activityLogRepository;
    }

    // =========================================================================
    // RefDataValidationLog — ACCOUNTING_RULES / JOURNAL_MAPPING
    // =========================================================================

    /**
     * Fetch reference-data validation logs with optional filters.
     *
     * @param jobId         (optional) batch job execution id
     * @param validationType (optional) ACCOUNTING_RULES | JOURNAL_MAPPING
     * @param sourceTable   (optional) e.g. "Aggregation", "AccountTypes"
     * @param errorCode     (optional) e.g. "ERR_REQ_01"
     */
    @GetMapping("/ref")
    public ResponseEntity<List<RefDataValidationLog>> getRefLogs(
            @RequestParam(required = false) Long jobId,
            @RequestParam(required = false) ValidationType validationType,
            @RequestParam(required = false) String sourceTable,
            @RequestParam(required = false) String errorCode) {

        try {
            String tenant = TenantContextHolder.getTenant();
            List<RefDataValidationLog> result = TenantContextHolder.runWithTenant(tenant, () -> {
                // Priority: jobId + secondary filter → jobId only → secondary filter only → all
                if (jobId != null && validationType != null) {
                    return refLogRepository.findByJobIdAndValidationType(jobId, validationType);
                }
                if (jobId != null && sourceTable != null) {
                    return refLogRepository.findByJobIdAndSourceTable(jobId, sourceTable);
                }
                if (jobId != null && errorCode != null) {
                    return refLogRepository.findByJobIdAndErrorCode(jobId, errorCode);
                }
                if (jobId != null) {
                    return refLogRepository.findByJobId(jobId);
                }
                if (validationType != null) {
                    return refLogRepository.findByValidationType(validationType);
                }
                if (sourceTable != null) {
                    return refLogRepository.findBySourceTable(sourceTable);
                }
                if (errorCode != null) {
                    return refLogRepository.findByErrorCode(errorCode);
                }
                return refLogRepository.findAll();
            });
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Failed to fetch RefDataValidationLog", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Fetch a single reference-data validation log by its MongoDB id.
     */
    @GetMapping("/ref/{id}")
    public ResponseEntity<RefDataValidationLog> getRefLogById(@PathVariable String id) {
        try {
            String tenant = TenantContextHolder.getTenant();
            return TenantContextHolder.runWithTenant(tenant, () ->
                    refLogRepository.findById(id)
                            .map(ResponseEntity::ok)
                            .orElse(ResponseEntity.notFound().build())
            );
        } catch (Exception e) {
            log.error("Failed to fetch RefDataValidationLog id={}", id, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Fetch all reference-data logs for a given {@link ValidationType}.
     *
     * <p>Path: {@code GET /ref/by-type/{validationType}}
     * <p>Valid values: {@code ACCOUNTING_RULES}, {@code JOURNAL_MAPPING}
     */
    @GetMapping("/ref/by-type/{validationType}")
    public ResponseEntity<List<RefDataValidationLog>> getRefLogsByType(
            @PathVariable ValidationType validationType) {
        try {
            String tenant = TenantContextHolder.getTenant();
            List<RefDataValidationLog> result = TenantContextHolder.runWithTenant(tenant,
                    () -> refLogRepository.findByValidationType(validationType));
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Failed to fetch RefDataValidationLog by type={}", validationType, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Fetch reference-data logs for a given {@link ValidationType} scoped to one batch job.
     *
     * <p>Path: {@code GET /ref/by-type/{validationType}/job/{jobId}}
     */
    @GetMapping("/ref/by-type/{validationType}/job/{jobId}")
    public ResponseEntity<List<RefDataValidationLog>> getRefLogsByTypeAndJob(
            @PathVariable ValidationType validationType,
            @PathVariable Long jobId) {
        try {
            String tenant = TenantContextHolder.getTenant();
            List<RefDataValidationLog> result = TenantContextHolder.runWithTenant(tenant,
                    () -> refLogRepository.findByJobIdAndValidationType(jobId, validationType));
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Failed to fetch RefDataValidationLog by type={} jobId={}", validationType, jobId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // =========================================================================
    // ActivityDataValidationLog — ACTIVITY / CUSTOM_ACTIVITY
    // =========================================================================

    /**
     * Fetch activity validation logs with optional filters.
     *
     * @param jobId          (optional) batch job execution id
     * @param validationType (optional) ACTIVITY | CUSTOM_ACTIVITY
     * @param errorCode      (optional) e.g. "ERR_REF_03"
     * @param instrumentId   (optional) e.g. "INST-001"
     */
    @GetMapping("/activity")
    public ResponseEntity<List<ActivityDataValidationLog>> getActivityLogs(
            @RequestParam(required = false) Long jobId,
            @RequestParam(required = false) ValidationType validationType,
            @RequestParam(required = false) String errorCode,
            @RequestParam(required = false) String instrumentId,
            @RequestParam(required = false) String attributeId,
            @RequestParam(required = false) Integer postingDate) {

        try {
            String tenant = TenantContextHolder.getTenant();
            List<ActivityDataValidationLog> result = TenantContextHolder.runWithTenant(tenant, () -> {
                ActivityDataValidationLog probe = new ActivityDataValidationLog();
                probe.setJobId(jobId);
                probe.setValidationType(validationType);
                probe.setErrorCode(errorCode);
                probe.setInstrumentId(instrumentId);
                probe.setAttributeId(attributeId);
                probe.setPostingDate(postingDate);

                ExampleMatcher matcher = ExampleMatcher.matching()
                        .withIgnoreNullValues()
                        .withIgnorePaths("createdAt");

                return activityLogRepository.findAll(Example.of(probe, matcher));
            });
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Failed to fetch ActivityDataValidationLog", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Fetch a single activity validation log by its MongoDB id.
     */
    @GetMapping("/activity/{id}")
    public ResponseEntity<ActivityDataValidationLog> getActivityLogById(@PathVariable String id) {
        try {
            String tenant = TenantContextHolder.getTenant();
            return TenantContextHolder.runWithTenant(tenant, () ->
                    activityLogRepository.findById(id)
                            .map(ResponseEntity::ok)
                            .orElse(ResponseEntity.notFound().build())
            );
        } catch (Exception e) {
            log.error("Failed to fetch ActivityDataValidationLog id={}", id, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Fetch all activity logs for a given {@link ValidationType}.
     *
     * <p>Path: {@code GET /activity/by-type/{validationType}}
     * <p>Valid values: {@code ACTIVITY}, {@code CUSTOM_ACTIVITY}
     */
    @GetMapping("/activity/by-type/{validationType}")
    public ResponseEntity<List<ActivityDataValidationLog>> getActivityLogsByType(
            @PathVariable ValidationType validationType) {
        try {
            String tenant = TenantContextHolder.getTenant();
            List<ActivityDataValidationLog> result = TenantContextHolder.runWithTenant(tenant,
                    () -> activityLogRepository.findByValidationType(validationType));
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Failed to fetch ActivityDataValidationLog by type={}", validationType, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Fetch activity logs for a given {@link ValidationType} scoped to one batch job.
     *
     * <p>Path: {@code GET /activity/by-type/{validationType}/job/{jobId}}
     */
    @GetMapping("/activity/by-type/{validationType}/job/{jobId}")
    public ResponseEntity<List<ActivityDataValidationLog>> getActivityLogsByTypeAndJob(
            @PathVariable ValidationType validationType,
            @PathVariable Long jobId) {
        try {
            String tenant = TenantContextHolder.getTenant();
            List<ActivityDataValidationLog> result = TenantContextHolder.runWithTenant(tenant,
                    () -> activityLogRepository.findByJobIdAndValidationType(jobId, validationType));
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Failed to fetch ActivityDataValidationLog by type={} jobId={}", validationType, jobId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // =========================================================================
    // Combined summary
    // =========================================================================

    /**
     * Returns a count summary for a given job across both collections.
     *
     * <pre>
     *   GET /summary?jobId={id}
     * </pre>
     */
    @GetMapping("/summary")
    public ResponseEntity<?> getSummary(@RequestParam Long jobId) {
        try {
            String tenant = TenantContextHolder.getTenant();
            return TenantContextHolder.runWithTenant(tenant, () -> {
                long refCount      = refLogRepository.findByJobId(jobId).size();
                long activityCount = activityLogRepository.findByJobId(jobId).size();

                java.util.Map<String, Object> summary = new java.util.LinkedHashMap<>();
                summary.put("jobId",                     jobId);
                summary.put("refDataValidationErrors",   refCount);
                summary.put("activityValidationErrors",  activityCount);
                summary.put("totalErrors",               refCount + activityCount);
                return ResponseEntity.ok(summary);
            });
        } catch (Exception e) {
            log.error("Failed to build validation summary for jobId={}", jobId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
