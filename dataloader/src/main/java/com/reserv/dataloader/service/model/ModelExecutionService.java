
package com.reserv.dataloader.service.model;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.entity.Event;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.entity.ModelExecutionBatchLog;
import com.fyntrac.common.repository.AttributeLevelBalanceRepository;
import com.fyntrac.common.repository.EventRepository;
import com.fyntrac.common.repository.GeneralLedgerEnteryStageRepository;
import com.fyntrac.common.repository.InstrumentAttributeRepository;
import com.fyntrac.common.repository.InstrumentLevelLtdRepository;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.repository.MetricLevelLtdRepository;
import com.fyntrac.common.repository.ModelExecutionBatchLogRepository;
import com.fyntrac.common.repository.TransactionActivityRepository;
import com.fyntrac.common.service.ExcelModelService;
import com.fyntrac.common.service.InstrumentAttributeService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.StringUtil;
import com.reserv.dataloader.pulsar.producer.ModelExecutionProducer;
import com.reserv.dataloader.pulsar.producer.PythonModelExecutionProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ModelExecutionService {

    @Value("${fyntrac.chunk.size}")
    private int chunkSize;

    private final InstrumentAttributeService instrumentAttributeService;
    private final MemcachedRepository memcachedRepository;
    private final ModelExecutionProducer modelExecutionProducer;
    private final PythonModelExecutionProducer pythonModelExecutionProducer;
    private final ExcelModelService excelModelService;
    private final EventRepository eventRepository;
    private final ModelExecutionBatchLogRepository batchLogRepository;

    // Repositories for pre-execution data cleanup
    private final TransactionActivityRepository transactionActivityRepository;
    private final InstrumentAttributeRepository instrumentAttributeRepository;
    private final GeneralLedgerEnteryStageRepository generalLedgerEnteryStageRepository;
    private final AttributeLevelBalanceRepository attributeLevelBalanceRepository;
    private final InstrumentLevelLtdRepository instrumentLevelLtdRepository;
    private final MetricLevelLtdRepository metricLevelLtdRepository;

    @Value("${fyntrac.chunk.size}")
    private int pageSize;

    /**
     * In-memory cache: "tenantId:postingDate" → total instrument count from EventHistory.
     *
     * Why NOT ThreadLocal:
     *   ThreadLocal is scoped to a single OS/platform thread and dies when the HTTP request
     *   finishes. The progress endpoint is called on many different threads across many
     *   HTTP requests, so ThreadLocal values would not survive between polls.
     *
     * Why ConcurrentHashMap:
     *   - Shared across all HTTP request threads (singleton service bean).
     *   - Thread-safe reads and writes without locking the entire map.
     *   - computeIfAbsent is atomic — the DB is hit at most once per (tenant, postingDate).
     *   - Keyed by tenantId + postingDate → correct isolation in multi-tenant architecture.
     *   - Invalidated (removed) whenever a new execution starts, ensuring freshness.
     */
    private final java.util.concurrent.ConcurrentHashMap<String, Long> instrumentCountCache =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Per-tenant execution lock.
     * Guarantees at most one active model execution per tenant at a time.
     * Uses ReentrantLock with tryLock() so the HTTP request thread returns
     * immediately (HTTP 409) instead of blocking if the tenant is already executing.
     */
    private final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.locks.ReentrantLock> tenantExecutionLocks =
            new java.util.concurrent.ConcurrentHashMap<>();

    /** Returns true if a lock was acquired (caller must release). False = tenant already executing. */
    public boolean tryAcquireExecutionLock(String tenantId) {
        java.util.concurrent.locks.ReentrantLock lock =
                tenantExecutionLocks.computeIfAbsent(tenantId, k -> new java.util.concurrent.locks.ReentrantLock());
        return lock.tryLock();
    }

    /** Releases the execution lock for the tenant. Safe to call even if lock is not held. */
    public void releaseExecutionLock(String tenantId) {
        java.util.concurrent.locks.ReentrantLock lock = tenantExecutionLocks.get(tenantId);
        if (lock != null && lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }

    @Autowired
    public ModelExecutionService(InstrumentAttributeService instrumentAttributeService
    , MemcachedRepository memcachedRepository
    , ModelExecutionProducer modelExecutionProducer
    , PythonModelExecutionProducer pythonModelExecutionProducer
    , ExcelModelService excelModelService
    , EventRepository eventRepository
    , ModelExecutionBatchLogRepository batchLogRepository
    , TransactionActivityRepository transactionActivityRepository
    , InstrumentAttributeRepository instrumentAttributeRepository
    , GeneralLedgerEnteryStageRepository generalLedgerEnteryStageRepository
    , AttributeLevelBalanceRepository attributeLevelBalanceRepository
    , InstrumentLevelLtdRepository instrumentLevelLtdRepository
    , MetricLevelLtdRepository metricLevelLtdRepository) {
        this.instrumentAttributeService = instrumentAttributeService;
        this.memcachedRepository = memcachedRepository;
        this.modelExecutionProducer = modelExecutionProducer;
        this.pythonModelExecutionProducer = pythonModelExecutionProducer;
        this.excelModelService = excelModelService;
        this.eventRepository = eventRepository;
        this.batchLogRepository = batchLogRepository;
        this.transactionActivityRepository = transactionActivityRepository;
        this.instrumentAttributeRepository = instrumentAttributeRepository;
        this.generalLedgerEnteryStageRepository = generalLedgerEnteryStageRepository;
        this.attributeLevelBalanceRepository = attributeLevelBalanceRepository;
        this.instrumentLevelLtdRepository = instrumentLevelLtdRepository;
        this.metricLevelLtdRepository = metricLevelLtdRepository;
    }

    /**
     * Purges all execution-output data for the given posting date across every
     * derived collection before a fresh model run.
     *
     * Collections cleaned:
     *   TransactionActivity, InstrumentAttribute, EventHistory,
     *   GeneralLedgerEnteryStage, AttributeLevelLtd,
     *   InstrumentLevelLtd, MetricLevelLtd
     *
     * Each delete is independent; a failure in one collection is logged but
     * does NOT prevent the remaining collections from being cleaned.
     */
    public void cleanupDataForPostingDate(int postingDate) {
        log.info("Pre-execution cleanup started for postingDate={} tenant={}", postingDate, TenantContextHolder.getTenant());

        // 1. TransactionActivity
        try {
            transactionActivityRepository.deleteByPostingDate(postingDate);
            log.debug("Cleanup: TransactionActivity deleted for postingDate={}", postingDate);
        } catch (Exception e) {
            log.error("Cleanup failed for TransactionActivity postingDate={}: {}", postingDate, e.getMessage());
        }

        // 2. InstrumentAttribute
        try {
            instrumentAttributeRepository.deleteByPostingDate(postingDate);
            log.debug("Cleanup: InstrumentAttribute deleted for postingDate={}", postingDate);
        } catch (Exception e) {
            log.error("Cleanup failed for InstrumentAttribute postingDate={}: {}", postingDate, e.getMessage());
        }

        // 3. EventHistory (mapped to Event entity)
        try {
            eventRepository.deleteByPostingDate(postingDate);
            log.debug("Cleanup: EventHistory deleted for postingDate={}", postingDate);
        } catch (Exception e) {
            log.error("Cleanup failed for EventHistory postingDate={}: {}", postingDate, e.getMessage());
        }

        // 4. GeneralLedgerEnteryStage
        try {
            generalLedgerEnteryStageRepository.deleteByPostingDate(postingDate);
            log.debug("Cleanup: GeneralLedgerEnteryStage deleted for postingDate={}", postingDate);
        } catch (Exception e) {
            log.error("Cleanup failed for GeneralLedgerEnteryStage postingDate={}: {}", postingDate, e.getMessage());
        }

        // 5. AttributeLevelLtd
        try {
            attributeLevelBalanceRepository.deleteByPostingDate(postingDate);
            log.debug("Cleanup: AttributeLevelLtd deleted for postingDate={}", postingDate);
        } catch (Exception e) {
            log.error("Cleanup failed for AttributeLevelLtd postingDate={}: {}", postingDate, e.getMessage());
        }

        // 6. InstrumentLevelLtd
        try {
            instrumentLevelLtdRepository.deleteByPostingDate(postingDate);
            log.debug("Cleanup: InstrumentLevelLtd deleted for postingDate={}", postingDate);
        } catch (Exception e) {
            log.error("Cleanup failed for InstrumentLevelLtd postingDate={}: {}", postingDate, e.getMessage());
        }

        // 7. MetricLevelLtd
        try {
            metricLevelLtdRepository.deleteByPostingDate(postingDate);
            log.debug("Cleanup: MetricLevelLtd deleted for postingDate={}", postingDate);
        } catch (Exception e) {
            log.error("Cleanup failed for MetricLevelLtd postingDate={}: {}", postingDate, e.getMessage());
        }

        log.info("Pre-execution cleanup completed for postingDate={} tenant={}", postingDate, TenantContextHolder.getTenant());
    }

    public void sendModelExecutionMessage(String date) throws Throwable {
        // Page request for chunk size
        int pageNumber = 0;
        boolean hasMoreData = true;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy"); // Define the format

        Date executionDate = DateUtil.parseDate(date, formatter);
        // Loop to fetch and process in chunks
        while (hasMoreData) {

            // Fetch the chunk of data
            List<InstrumentAttribute> chunk = this.instrumentAttributeService.getDistinctInstrumentsByInstrumentId(null, pageNumber, chunkSize);

            Set<String> instrumentIdChunk = new HashSet<>(0);
            for(InstrumentAttribute instrumentAttribute: chunk) {
                instrumentIdChunk.add(instrumentAttribute.getInstrumentId());
            }

            if (!chunk.isEmpty()) {
                // Process this chunk, e.g., send it to your REST API
                //send message to consumen
                boolean isLastPage = Boolean.FALSE;
                this.postModelExecutionMessage(executionDate, new ArrayList<>(instrumentIdChunk), pageNumber, isLastPage);
                pageNumber++;  // Move to the next page
            } else {
                // No more data to fetch
                hasMoreData = false;
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════════
    // Streaming pipeline API — prepare / dispatchBatch / finalize
    //
    // The controller calls:
    //   1. prepare*(date, postingDate)   — initialise job state, seed cache
    //   2. dispatch*Batch(executionDate, instrumentIds) — called per page from
    //      ExcelModelService.generateEventAndDispatch()  (callback)
    //   3. finalize*()                  — write EXECUTION_SUMMARY log
    //
    // Memory model: only one page's Set<String> is alive at a time.
    // Scales to millions of instruments without heap accumulation.
    // ══════════════════════════════════════════════════════════════════════════════

    // ── Shared streaming state (per-request, initialised by prepare*) ─────────────
    // These fields are intentionally NOT ThreadLocal — they are initialised on the
    // HTTP request thread by prepare*() and read/written from the same thread
    // (or from the virtual-thread callbacks which complete before finalize*() runs).
    private volatile String  streamJobId;
    private volatile String  streamTenant;
    private volatile int     streamPostingDate;
    private volatile long    streamOverallStart;
    private volatile java.util.concurrent.atomic.AtomicInteger streamPageCounter;
    private final    java.util.concurrent.CopyOnWriteArrayList<Boolean>  excelBatchOutcomes     = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final    java.util.concurrent.CopyOnWriteArrayList<Integer>  excelInstrumentCounts  = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final    java.util.concurrent.CopyOnWriteArrayList<BatchResult> pythonBatchResults   = new java.util.concurrent.CopyOnWriteArrayList<>();

    /** Called once before generateEventAndDispatch() for the Excel path. */
    public void prepareExcelExecution(String date, int postingDate) {
        streamTenant       = TenantContextHolder.getTenant();
        streamJobId        = String.valueOf(System.currentTimeMillis());
        streamPostingDate  = postingDate;
        streamOverallStart = System.currentTimeMillis();
        streamPageCounter  = new java.util.concurrent.atomic.AtomicInteger(0);
        excelBatchOutcomes.clear();
        excelInstrumentCounts.clear();
        instrumentCountCache.remove(streamTenant + ":" + postingDate);
        log.info("Excel streaming execution prepared: jobId={} tenant={} postingDate={}", streamJobId, streamTenant, postingDate);
    }

    /** Dispatches one page of instrument IDs synchronously. Called from the callback. */
    public void dispatchExcelBatch(Date executionDate, Set<String> instrumentIds) {
        if (instrumentIds == null || instrumentIds.isEmpty()) return;
        long batchStart = System.currentTimeMillis();
        int page = streamPageCounter.getAndIncrement();
        List<String> idList = new ArrayList<>(instrumentIds);
        boolean success = false;
        try {
            TenantContextHolder.runWithTenant(streamTenant, () -> {
                postModelExecutionMessage(executionDate, idList, page, false /* isLast handled by finalize */);
                return null;
            });
            success = true;
        } catch (Exception e) {
            log.error("Excel streaming batch dispatch failed for page {}: {}", page, e.getMessage());
        } finally {
            long durationMs = System.currentTimeMillis() - batchStart;
            String status = success ? "SUCCESS" : "FAILED";
            try {
                ModelExecutionBatchLog batchLog = ModelExecutionBatchLog.builder()
                        .jobId(streamJobId).batchNumber(page).tenantId(streamTenant)
                        .postingDate(streamPostingDate).modelType("EXCEL").logType("EXECUTION_BATCH")
                        .instrumentIds(idList).instrumentCount(idList.size())
                        .successCount(success ? idList.size() : 0)
                        .failedCount(success ? 0 : idList.size())
                        .status(status).durationMs(durationMs).createdAt(new Date()).build();
                final boolean finalSuccess = success;
                TenantContextHolder.runWithTenant(streamTenant, () -> { batchLogRepository.save(batchLog); return null; });
            } catch (Exception logEx) {
                log.warn("Failed to save Excel EXECUTION_BATCH for page {}: {}", page, logEx.getMessage());
            }
            excelBatchOutcomes.add(success);
            excelInstrumentCounts.add(idList.size());
        }
    }

    /** Called once after generateEventAndDispatch() completes for Excel. Writes summary log and releases tenant lock. */
    public void finalizeExcelExecution() {
        try {
            long overallDuration = System.currentTimeMillis() - streamOverallStart;
            long successBatches  = excelBatchOutcomes.stream().filter(Boolean::booleanValue).count();
            long failedBatches   = excelBatchOutcomes.size() - successBatches;
            int  totalInstruments = excelInstrumentCounts.stream().mapToInt(Integer::intValue).sum();
            String summaryStatus = failedBatches == 0 ? "SUCCESS" : "PARTIAL_SUCCESS";
            try {
                ModelExecutionBatchLog summary = ModelExecutionBatchLog.builder()
                        .jobId(streamJobId).tenantId(streamTenant).postingDate(streamPostingDate)
                        .modelType("EXCEL").logType("EXECUTION_SUMMARY")
                        .instrumentCount(totalInstruments).successCount((int) successBatches)
                        .failedCount((int) failedBatches).status(summaryStatus)
                        .durationMs(overallDuration).createdAt(new Date()).build();
                batchLogRepository.save(summary);
                log.info("Excel EXECUTION_SUMMARY: jobId={} status={} batches={} instruments={} duration={}ms",
                        streamJobId, summaryStatus, excelBatchOutcomes.size(), totalInstruments, overallDuration);
            } catch (Exception e) {
                log.warn("Failed to save Excel EXECUTION_SUMMARY: {}", e.getMessage());
            }
        } finally {
            releaseExecutionLock(streamTenant);
        }
    }

    /** Called once before generateEventAndDispatch() for the Python path. */
    public void preparePythonExecution(String date, int postingDate) {
        streamTenant       = TenantContextHolder.getTenant();
        streamJobId        = String.valueOf(System.currentTimeMillis());
        streamPostingDate  = postingDate;
        streamOverallStart = System.currentTimeMillis();
        streamPageCounter  = new java.util.concurrent.atomic.AtomicInteger(0);
        pythonBatchResults.clear();
        instrumentCountCache.remove(streamTenant + ":" + postingDate);
        log.info("Python streaming execution prepared: jobId={} tenant={} postingDate={}", streamJobId, streamTenant, postingDate);
    }

    /** Dispatches one page of instrument IDs synchronously. Called from the callback. */
    public void dispatchPythonBatch(Date executionDate, Set<String> instrumentIds) {
        if (instrumentIds == null || instrumentIds.isEmpty()) return;
        int page = streamPageCounter.getAndIncrement();
        long batchStart = System.currentTimeMillis();
        List<String> idList = new ArrayList<>(instrumentIds);
        boolean success = false;
        String errorMsg = null;
        try {
            TenantContextHolder.runWithTenant(streamTenant, () -> {
                postPythonModelExecutionMessage(executionDate, idList, page, false);
                return null;
            });
            success = true;
        } catch (Exception e) {
            errorMsg = e.getMessage();
            log.error("Python streaming batch dispatch failed for page {}: {}", page, e.getMessage());
        } finally {
            long duration = System.currentTimeMillis() - batchStart;
            String status = success ? "SUCCESS" : "FAILED";
            final String capturedErrorMsg = errorMsg;
            try {
                ModelExecutionBatchLog batchLog = ModelExecutionBatchLog.builder()
                        .jobId(streamJobId).batchNumber(page).tenantId(streamTenant)
                        .postingDate(streamPostingDate).modelType("PYTHON").logType("EXECUTION_BATCH")
                        .instrumentIds(idList).instrumentCount(idList.size())
                        .successCount(success ? idList.size() : 0)
                        .failedCount(success ? 0 : idList.size())
                        .status(status).errorMessage(capturedErrorMsg)
                        .durationMs(duration).createdAt(new Date()).build();
                TenantContextHolder.runWithTenant(streamTenant, () -> { batchLogRepository.save(batchLog); return null; });
            } catch (Exception logEx) {
                log.warn("Failed to save Python EXECUTION_BATCH for page {}: {}", page, logEx.getMessage());
            }
            pythonBatchResults.add(new BatchResult(page, success, duration, idList.size(), errorMsg));
        }
    }

    /** Called once after generateEventAndDispatch() completes for Python. Writes summary log and releases tenant lock. */
    public void finalizePythonExecution() {
        try {
            long overallDuration = System.currentTimeMillis() - streamOverallStart;
            long successBatches  = pythonBatchResults.stream().filter(BatchResult::success).count();
            long failedBatches   = pythonBatchResults.size() - successBatches;
            int  totalInstruments = pythonBatchResults.stream().mapToInt(BatchResult::itemsSent).sum();
            String summaryStatus = failedBatches == 0 ? "SUCCESS" : "PARTIAL_SUCCESS";
            try {
                ModelExecutionBatchLog summary = ModelExecutionBatchLog.builder()
                        .jobId(streamJobId).tenantId(streamTenant).postingDate(streamPostingDate)
                        .modelType("PYTHON").logType("EXECUTION_SUMMARY")
                        .instrumentCount(totalInstruments).successCount((int) successBatches)
                        .failedCount((int) failedBatches).status(summaryStatus)
                        .durationMs(overallDuration).createdAt(new Date()).build();
                batchLogRepository.save(summary);
                log.info("Python EXECUTION_SUMMARY: jobId={} status={} batches={} instruments={} duration={}ms",
                        streamJobId, summaryStatus, pythonBatchResults.size(), totalInstruments, overallDuration);
            } catch (Exception e) {
                log.warn("Failed to save Python EXECUTION_SUMMARY: {}", e.getMessage());
            }
        } finally {
            releaseExecutionLock(streamTenant);
        }
    }


    /**
     * Legacy signature — falls back to DB-based pagination when no pre-collected batches are available.
     */
    public void sendExcelModelExecutionMessage(String date) throws Throwable {
        sendExcelModelExecutionMessage(date, null);
    }

    /**
     * Merged execution: accepts pre-collected instrument batches from generateEventAndCollectInstruments().
     * When instrumentBatches is non-null, skips the second MongoDB pagination entirely.
     */
    public void sendExcelModelExecutionMessage(String date, List<Set<String>> instrumentBatches) throws Throwable {
        final String tenant = TenantContextHolder.getTenant();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        Date executionDate = DateUtil.parseDate(date, formatter);
        int postingDateNumber = DateUtil.dateInNumber(executionDate);
        final String jobId = String.valueOf(System.currentTimeMillis());
        long overallStart = System.currentTimeMillis();

        // ── Resolve total instruments and batches ──────────────────────────────────
        long distinctInstruments;
        int totalPages;

        if (instrumentBatches != null && !instrumentBatches.isEmpty()) {
            // Pre-collected: no DB query needed
            distinctInstruments = instrumentBatches.stream().mapToInt(Set::size).sum();
            totalPages = instrumentBatches.size();
        } else {
            // Fallback: query DB for distinct instruments
            Long count = TenantContextHolder.runWithTenant(tenant,
                    () -> eventRepository.countDistinctInstrumentsByPostingDate(postingDateNumber));
            distinctInstruments = count != null ? count : 0L;
            totalPages = pageSize > 0 ? (int) Math.ceil((double) distinctInstruments / pageSize) : 0;
        }

        if (distinctInstruments == 0) {
            log.info("No instruments found for posting date {} and tenant {}", postingDateNumber, tenant);
            return;
        }

        // ── Seed progress cache ───────────────────────────────────────────────────
        String cacheKey = tenant + ":" + postingDateNumber;
        instrumentCountCache.put(cacheKey, distinctInstruments);

        final List<Boolean> batchOutcomes = new java.util.concurrent.CopyOnWriteArrayList<>();
        final List<Integer> totalInstrumentsSent = new java.util.concurrent.CopyOnWriteArrayList<>();
        String summaryError = null;
        final int finalTotalPages = totalPages;

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Semaphore semaphore = new Semaphore(10);
            List<CompletableFuture<Boolean>> futures = new ArrayList<>();

            for (int pageNumber = 0; pageNumber < finalTotalPages; pageNumber++) {
                final int currentPage = pageNumber;

                CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
                    long batchStart = System.currentTimeMillis();
                    List<String> instrumentIdList = new ArrayList<>();
                    String errorMsg = null;
                    boolean batchSuccess = false;
                    try {
                        semaphore.acquire();
                        batchSuccess = TenantContextHolder.runWithTenant(tenant, () -> {
                            try {
                                Set<String> instrumentIdChunk;
                                if (instrumentBatches != null) {
                                    // Use pre-collected batch — zero DB reads
                                    instrumentIdChunk = instrumentBatches.get(currentPage);
                                } else {
                                    // Fallback: query Event collection
                                    Page<Event> page = this.eventRepository.findInstrumentIdsByPostingDateAndStatusNotStarted(
                                            postingDateNumber, PageRequest.of(currentPage, pageSize));
                                    instrumentIdChunk = page.getContent().stream()
                                            .map(Event::getInstrumentId)
                                            .filter(Objects::nonNull)
                                            .collect(Collectors.toSet());
                                }
                                if (!instrumentIdChunk.isEmpty()) {
                                    instrumentIdList.addAll(instrumentIdChunk);
                                    this.postModelExecutionMessage(executionDate, new ArrayList<>(instrumentIdChunk),
                                            currentPage, (currentPage == finalTotalPages - 1));
                                    return true;
                                }
                                return false;
                            } catch (Exception e) {
                                log.error("Failed to process page {}: {}", currentPage, e.getMessage());
                                return false;
                            } finally {
                                semaphore.release();
                            }
                        });
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        errorMsg = "Interrupted";
                    } catch (Exception e) {
                        errorMsg = e.getMessage();
                    } finally {
                        long durationMs = System.currentTimeMillis() - batchStart;
                        String status = batchSuccess ? "SUCCESS" : "FAILED";
                        final String capturedErrorMsg = errorMsg;
                        try {
                            ModelExecutionBatchLog batchLog = ModelExecutionBatchLog.builder()
                                    .jobId(jobId).batchNumber(currentPage).tenantId(tenant)
                                    .postingDate(postingDateNumber).modelType("EXCEL").logType("EXECUTION_BATCH")
                                    .instrumentIds(instrumentIdList).instrumentCount(instrumentIdList.size())
                                    .successCount(batchSuccess ? instrumentIdList.size() : 0)
                                    .failedCount(batchSuccess ? 0 : instrumentIdList.size())
                                    .status(status).errorMessage(capturedErrorMsg)
                                    .durationMs(durationMs).createdAt(new Date()).build();
                            TenantContextHolder.runWithTenant(tenant, () -> { batchLogRepository.save(batchLog); return null; });
                        } catch (Exception logEx) {
                            log.warn("Failed to save Excel EXECUTION_BATCH for page {}: {}", currentPage, logEx.getMessage());
                        }
                        batchOutcomes.add(batchSuccess);
                        totalInstrumentsSent.add(instrumentIdList.size());
                    }
                    return batchSuccess;
                }, executor);
                futures.add(future);
            }

            List<Boolean> results = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
            long successCount = results.stream().filter(Boolean::booleanValue).count();
            log.info("Excel model: {}/{} batches successful for tenant {}", successCount, finalTotalPages, tenant);
        } catch (Exception ex) {
            summaryError = ex.getMessage();
            log.error("Excel model execution failed for tenant {}", tenant, ex);
            throw new RuntimeException("Event generation failed for tenant " + tenant, ex);
        } finally {
            long overallDuration = System.currentTimeMillis() - overallStart;
            long successBatches  = batchOutcomes.stream().filter(Boolean::booleanValue).count();
            long failedBatches   = batchOutcomes.size() - successBatches;
            int  totalInstruments = totalInstrumentsSent.stream().mapToInt(Integer::intValue).sum();
            String summaryStatus = summaryError != null ? "FAILED"
                    : (failedBatches == 0 ? "SUCCESS" : "PARTIAL_SUCCESS");
            try {
                ModelExecutionBatchLog summary = ModelExecutionBatchLog.builder()
                        .jobId(jobId).tenantId(tenant).postingDate(postingDateNumber)
                        .modelType("EXCEL").logType("EXECUTION_SUMMARY")
                        .instrumentCount(totalInstruments).successCount((int) successBatches)
                        .failedCount((int) failedBatches).status(summaryStatus)
                        .errorMessage(summaryError).durationMs(overallDuration).createdAt(new Date()).build();
                batchLogRepository.save(summary);
                log.info("Excel EXECUTION_SUMMARY saved: jobId={} status={} batches={} instruments={} duration={}ms",
                        jobId, summaryStatus, batchOutcomes.size(), totalInstruments, overallDuration);
            } catch (Exception logEx) {
                log.warn("Failed to save Excel EXECUTION_SUMMARY: {}", logEx.getMessage());
            }
        }
    }

    private void postModelExecutionMessage(Date executionDate, List<String> instruments, int page, boolean isLast) {
        CacheList<String> cacheList = new CacheList<>();
        instruments.forEach(cacheList::add);
        int hashCode = Objects.hash(cacheList);
        String tenantId = TenantContextHolder.getTenant();
        String key = "Model" + tenantId + hashCode;
        this.memcachedRepository.putInCache(key, cacheList);
        this.modelExecutionProducer.sendModelExecutionMessage(RecordFactory.createModelExecutionMessage(tenantId,
                DateUtil.dateInNumber(executionDate), key, isLast));
        // Collect into CacheList

    }

    /**
     * Legacy signature — falls back to DB-based pagination.
     */
    public void sendPythonModelExecutionMessage(String date) throws Throwable {
        sendPythonModelExecutionMessage(date, null);
    }

    /**
     * Merged execution: accepts pre-collected instrument batches from generateEventAndCollectInstruments().
     * When instrumentBatches is non-null, skips the second MongoDB pagination entirely.
     */
    public void sendPythonModelExecutionMessage(String date, List<Set<String>> instrumentBatches) throws Throwable {
        final String tenant = TenantContextHolder.getTenant();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        Date executionDate = DateUtil.parseDate(date, formatter);
        int postingDateNumber = DateUtil.dateInNumber(executionDate);
        final String jobId = String.valueOf(System.currentTimeMillis());

        // ── Resolve total instruments and batches ──────────────────────────────────
        long distinctInstruments;
        int totalPages;

        if (instrumentBatches != null && !instrumentBatches.isEmpty()) {
            distinctInstruments = instrumentBatches.stream().mapToInt(Set::size).sum();
            totalPages = instrumentBatches.size();
        } else {
            Long count = TenantContextHolder.runWithTenant(tenant,
                    () -> eventRepository.countDistinctInstrumentsByPostingDate(postingDateNumber));
            distinctInstruments = count != null ? count : 0L;
            totalPages = pageSize > 0 ? (int) Math.ceil((double) distinctInstruments / pageSize) : 0;
        }

        if (distinctInstruments == 0) {
            log.info("No events found for Python model execution. PostingDate={}, Tenant={}", postingDateNumber, tenant);
            return;
        }

        long overallStartTime = System.currentTimeMillis();
        final int finalTotalPages = totalPages;

        String cacheKey = tenant + ":" + postingDateNumber;
        instrumentCountCache.put(cacheKey, distinctInstruments);

        List<BatchResult> results = new ArrayList<>();
        String summaryError = null;

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Semaphore semaphore = new Semaphore(10);
            List<CompletableFuture<BatchResult>> futures = new ArrayList<>();

            for (int pageNumber = 0; pageNumber < finalTotalPages; pageNumber++) {
                final int currentPage = pageNumber;

                CompletableFuture<BatchResult> future = CompletableFuture.supplyAsync(() -> {
                    long batchStartTime = System.currentTimeMillis();
                    BatchResult batchResult = null;
                    List<String> instrumentIdList = new ArrayList<>();
                    try {
                        semaphore.acquire();
                        batchResult = TenantContextHolder.runWithTenant(tenant, () -> {
                            try {
                                Set<String> instrumentIdChunk;
                                if (instrumentBatches != null) {
                                    instrumentIdChunk = instrumentBatches.get(currentPage);
                                } else {
                                    Page<Event> page = this.eventRepository.findInstrumentIdsByPostingDateAndStatusNotStarted(
                                            postingDateNumber, PageRequest.of(currentPage, pageSize));
                                    instrumentIdChunk = page.getContent().stream()
                                            .map(Event::getInstrumentId)
                                            .filter(Objects::nonNull)
                                            .collect(Collectors.toSet());
                                }
                                if (!instrumentIdChunk.isEmpty()) {
                                    instrumentIdList.addAll(instrumentIdChunk);
                                    this.postPythonModelExecutionMessage(executionDate, new ArrayList<>(instrumentIdChunk),
                                            currentPage, (currentPage == finalTotalPages - 1));
                                    long duration = System.currentTimeMillis() - batchStartTime;
                                    return new BatchResult(currentPage, true, duration, instrumentIdChunk.size(), null);
                                }
                                long duration = System.currentTimeMillis() - batchStartTime;
                                return new BatchResult(currentPage, true, duration, 0, null);
                            } catch (Exception e) {
                                long duration = System.currentTimeMillis() - batchStartTime;
                                log.error("Python model: failed to process page {}: {}", currentPage, e.getMessage());
                                return new BatchResult(currentPage, false, duration, 0, e.getMessage());
                            } finally {
                                semaphore.release();
                            }
                        });
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        long duration = System.currentTimeMillis() - batchStartTime;
                        batchResult = new BatchResult(currentPage, false, duration, 0, "Interrupted");
                    } finally {
                        if (batchResult == null) {
                            long duration = System.currentTimeMillis() - batchStartTime;
                            batchResult = new BatchResult(currentPage, false, duration, 0, "Unknown error");
                        }
                        String status = batchResult.success() ? "SUCCESS" : "FAILED";
                        final BatchResult finalBatchResult = batchResult;
                        try {
                            ModelExecutionBatchLog batchLog = ModelExecutionBatchLog.builder()
                                    .jobId(jobId).batchNumber(currentPage).tenantId(tenant)
                                    .postingDate(postingDateNumber).modelType("PYTHON").logType("EXECUTION_BATCH")
                                    .instrumentIds(instrumentIdList).instrumentCount(instrumentIdList.size())
                                    .successCount(finalBatchResult.success() ? instrumentIdList.size() : 0)
                                    .failedCount(finalBatchResult.success() ? 0 : instrumentIdList.size())
                                    .status(status).errorMessage(finalBatchResult.errorMessage())
                                    .durationMs(finalBatchResult.durationMs()).createdAt(new Date()).build();
                            TenantContextHolder.runWithTenant(tenant, () -> { batchLogRepository.save(batchLog); return null; });
                        } catch (Exception logEx) {
                            log.warn("Failed to save Python EXECUTION_BATCH for page {}: {}", currentPage, logEx.getMessage());
                        }
                    }
                    return batchResult;
                }, executor);
                futures.add(future);
            }

            results.addAll(futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));

            long successCount = results.stream().filter(BatchResult::success).count();
            long totalItemsSent = results.stream().mapToInt(BatchResult::itemsSent).sum();
            long overallDuration = System.currentTimeMillis() - overallStartTime;

            log.info("Python model dispatch: {}/{} batches OK, {} instruments, {}ms for tenant {}",
                    successCount, finalTotalPages, totalItemsSent, overallDuration, tenant);
        } catch (Exception ex) {
            summaryError = ex.getMessage();
            log.error("Python model execution failed for tenant {}", tenant, ex);
            throw new RuntimeException("Python model execution failed for tenant " + tenant, ex);
        } finally {
            long overallDuration = System.currentTimeMillis() - overallStartTime;
            long successBatches  = results.stream().filter(BatchResult::success).count();
            long failedBatches   = results.size() - successBatches;
            int  totalInstruments = results.stream().mapToInt(BatchResult::itemsSent).sum();
            String summaryStatus = summaryError != null ? "FAILED"
                    : (failedBatches == 0 ? "SUCCESS" : "PARTIAL_SUCCESS");
            try {
                ModelExecutionBatchLog summary = ModelExecutionBatchLog.builder()
                        .jobId(jobId).tenantId(tenant).postingDate(postingDateNumber)
                        .modelType("PYTHON").logType("EXECUTION_SUMMARY")
                        .instrumentCount(totalInstruments).successCount((int) successBatches)
                        .failedCount((int) failedBatches).status(summaryStatus)
                        .errorMessage(summaryError).durationMs(overallDuration).createdAt(new Date()).build();
                batchLogRepository.save(summary);
                log.info("Python EXECUTION_SUMMARY saved: jobId={} status={} batches={} instruments={} duration={}ms",
                        jobId, summaryStatus, results.size(), totalInstruments, overallDuration);
            } catch (Exception logEx) {
                log.warn("Failed to save Python EXECUTION_SUMMARY: {}", logEx.getMessage());
            }
        }
    }

    private void postPythonModelExecutionMessage(Date executionDate, List<String> instruments, int page, boolean isLast) {
        String tenantId = TenantContextHolder.getTenant();
        this.pythonModelExecutionProducer.sendPythonModelExecutionMessage(
                RecordFactory.createPythonModelExecutionMessage(tenantId,
                        DateUtil.dateInNumber(executionDate), instruments, isLast));
        log.debug("Python model: published {} instruments for tenant={} date={} isLast={}",
                instruments.size(), tenantId, DateUtil.dateInNumber(executionDate), isLast);
    }

    public void executeMode(int executionDate) throws Throwable{
        try {
            excelModelService.generateEvent(executionDate);
           } catch (Throwable e) {
            // log.error(StringUtil.getStackTrace(e));
            throw  new RuntimeException(e.getLocalizedMessage());
        }
    }

    /**
     * Returns real-time execution progress for the current posting date.
     *
     * Performance notes:
     *  - Uses COUNT queries only (no document loading) for progress metrics.
     *    All counts are served from the compound index {tenantId, postingDate, logType, status}.
     *  - Three MongoDB count operations run in parallel via CompletableFuture.
     *  - EventHistory count is also index-served (postingDate index).
     *  - The batch timeline (for UI) is fetched only when includeBatches=true,
     *    keeping the lightweight polling path lean.
     *  - This method is entirely read-only and runs on the HTTP thread pool,
     *    completely isolated from the execution virtual-thread pool.
     */
    public Map<String, Object> getExecutionProgress(String tenant, Integer postingDate, boolean includeBatches) {

        // ── 1. Resolve totalInstruments from cache (zero DB on warm cache) ────────
        //    Cache is seeded when execution starts (sendExcel/PythonModelExecutionMessage).
        //    Falls back to DB only on cold cache (e.g. first poll before execution seeds it,
        //    or after a service restart). computeIfAbsent is atomic — DB hit at most once.
        String cacheKey = tenant + ":" + postingDate;
        long totalInstruments = instrumentCountCache.computeIfAbsent(cacheKey, k -> {
            log.debug("Progress cache miss for key={}; querying distinct instruments from EventHistory", k);
            try {
                // Count DISTINCT instrumentIds — not raw event rows.
                // One instrument can have many event rows; we want the actual instrument count.
                Long count = TenantContextHolder.runWithTenant(tenant,
                        () -> eventRepository.countDistinctInstrumentsByPostingDate(postingDate));
                return count != null ? count : 0L;
            } catch (Exception e) {
                log.warn("Could not count distinct instruments for postingDate={}: {}", postingDate, e.getMessage());
                return 0L;
            }
        });

        // ── 2. Parallel count queries (index-only, no document reads) ─────────────
        CompletableFuture<Long> completedFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return TenantContextHolder.runWithTenant(tenant,
                        () -> batchLogRepository.countByTenantIdAndPostingDateAndLogType(
                                tenant, postingDate, "EXECUTION_BATCH"));
            } catch (Exception e) {
                log.warn("Could not count EXECUTION_BATCH logs: {}", e.getMessage());
                return 0L;
            }
        });

        CompletableFuture<Long> successFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return TenantContextHolder.runWithTenant(tenant,
                        () -> batchLogRepository.countByTenantIdAndPostingDateAndLogTypeAndStatus(
                                tenant, postingDate, "EXECUTION_BATCH", "SUCCESS"));
            } catch (Exception e) {
                return 0L;
            }
        });

        CompletableFuture<Boolean> summaryFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return TenantContextHolder.runWithTenant(tenant,
                        () -> batchLogRepository.countByTenantIdAndPostingDateAndLogType(
                                tenant, postingDate, "EXECUTION_SUMMARY") > 0);
            } catch (Exception e) {
                return false;
            }
        });

        // ── Batch timeline fetch (only when UI explicitly requests it) ───────────
        CompletableFuture<List<ModelExecutionBatchLog>> batchListFuture = includeBatches
                ? CompletableFuture.supplyAsync(() -> {
                    try {
                        return TenantContextHolder.runWithTenant(tenant,
                                () -> batchLogRepository.findByTenantIdAndPostingDateAndLogType(
                                        tenant, postingDate, "EXECUTION_BATCH"));
                    } catch (Exception e) {
                        return Collections.<ModelExecutionBatchLog>emptyList();
                    }
                })
                : CompletableFuture.completedFuture(Collections.emptyList());

        // ── Wait for parallel count results ──────────────────────────────────────
        long completedBatches  = completedFuture.join();
        long successBatches    = successFuture.join();
        long failedBatches     = completedBatches - successBatches;
        boolean hasSummary     = summaryFuture.join();
        List<ModelExecutionBatchLog> batchList = batchListFuture.join();

        long totalInstrumentsProcessed = batchList.stream()
                .mapToInt(l -> l.getInstrumentCount() != null ? l.getInstrumentCount() : 0).sum();

        // Since batch size from the callback is independent of pageSize, we cannot accurately
        // predict totalExpectedBatches. Set it to 0 so the UI displays '?'
        int totalExpectedBatches = 0;

        boolean isComplete = hasSummary;

        double pct = totalInstruments > 0
                ? Math.min(100.0, ((double) totalInstrumentsProcessed * 100.0) / totalInstruments)
                : (completedBatches > 0 ? 100.0 : 0.0);

        if (isComplete) {
            pct = 100.0;
        }

        // ── Build response ───────────────────────────────────────────────────────
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tenantId",                  tenant);
        result.put("postingDate",               postingDate);
        result.put("pageSize",                  pageSize);
        result.put("totalInstruments",          totalInstruments);
        result.put("totalExpectedBatches",      totalExpectedBatches);
        result.put("completedBatches",          completedBatches);
        result.put("successBatches",            successBatches);
        result.put("failedBatches",             Math.max(0, failedBatches));
        result.put("totalInstrumentsProcessed", totalInstrumentsProcessed);
        result.put("completionPct",             Math.round(pct * 10.0) / 10.0);
        result.put("isComplete",                isComplete);

        if (includeBatches) {
            List<Map<String, Object>> batchSummaries = batchList.stream()
                    .sorted(Comparator.comparingInt(l -> l.getBatchNumber() != null ? l.getBatchNumber() : 0))
                    .map(l -> {
                        Map<String, Object> b = new LinkedHashMap<>();
                        b.put("batchNumber",     l.getBatchNumber());
                        b.put("jobId",           l.getJobId());
                        b.put("modelType",       l.getModelType());
                        b.put("instrumentCount", l.getInstrumentCount());
                        b.put("successCount",    l.getSuccessCount());
                        b.put("failedCount",     l.getFailedCount());
                        b.put("status",          l.getStatus());
                        b.put("durationMs",      l.getDurationMs());
                        b.put("errorMessage",    l.getErrorMessage());
                        b.put("createdAt",       l.getCreatedAt());
                        return b;
                    })
                    .collect(Collectors.toList());
            result.put("batches", batchSummaries);
        }

        return result;
    }

    private record BatchResult(int pageNumber, boolean success, long durationMs, int itemsSent, String errorMessage) {}
}
