package com.fyntrac.common.service;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ExcelModelServiceDispatchTest {

    // ---------------------------------------------------------------- splitIntoBatches

    @Test
    void splitPreservesOrderAndCoversEveryId() {
        Set<String> ids = new LinkedHashSet<>(List.of("A", "B", "C", "D", "E"));

        List<Set<String>> batches = ExcelModelService.splitIntoBatches(ids, 2);

        assertEquals(3, batches.size());
        assertEquals(List.of("A", "B"), new ArrayList<>(batches.get(0)));
        assertEquals(List.of("C", "D"), new ArrayList<>(batches.get(1)));
        assertEquals(List.of("E"),      new ArrayList<>(batches.get(2)));
    }

    @Test
    void splitKeepsOneBatchWhenPageFitsDispatchSize() {
        Set<String> ids = new LinkedHashSet<>(List.of("A", "B", "C"));

        List<Set<String>> batches = ExcelModelService.splitIntoBatches(ids, 500);

        assertEquals(1, batches.size());
        assertEquals(ids, batches.get(0));
    }

    @Test
    void splitTreatsNonPositiveSizeAsOne() {
        Set<String> ids = new LinkedHashSet<>(List.of("A", "B"));

        assertEquals(2, ExcelModelService.splitIntoBatches(ids, 0).size());
    }

    // ---------------------------------------------------------------- awaitDispatches

    @Test
    void awaitReturnsOnceAllDispatchesComplete() throws Exception {
        AtomicInteger completed = new AtomicInteger();
        AtomicLong lastProgress = new AtomicLong(System.nanoTime());
        List<CompletableFuture<Void>> futures = List.of(
                dispatch(50, completed, lastProgress),
                dispatch(100, completed, lastProgress));

        ExcelModelService.awaitDispatches(futures, completed, lastProgress,
                TimeUnit.SECONDS.toNanos(5), "TNT");

        assertEquals(2, completed.get());
    }

    @Test
    void awaitFailsWhenNothingCompletesWithinStallWindow() {
        AtomicInteger completed = new AtomicInteger();
        AtomicLong lastProgress = new AtomicLong(System.nanoTime());
        List<CompletableFuture<Void>> futures = List.of(new CompletableFuture<>()); // never completes

        TimeoutException ex = assertThrows(TimeoutException.class, () ->
                ExcelModelService.awaitDispatches(futures, completed, lastProgress,
                        TimeUnit.MILLISECONDS.toNanos(200), "TNT"));

        assertTrue(ex.getMessage().contains("0 of 1 batches completed"), ex.getMessage());
    }

    @Test
    void awaitRestartsStallClockOnEveryCompletion() throws Exception {
        // Total run (350ms) exceeds the stall window (250ms), but a batch completes at 150ms,
        // so no single silent stretch is longer than the window — this must NOT time out.
        AtomicInteger completed = new AtomicInteger();
        AtomicLong lastProgress = new AtomicLong(System.nanoTime());
        List<CompletableFuture<Void>> futures = List.of(
                dispatch(150, completed, lastProgress),
                dispatch(350, completed, lastProgress));

        assertDoesNotThrow(() -> ExcelModelService.awaitDispatches(futures, completed, lastProgress,
                TimeUnit.MILLISECONDS.toNanos(250), "TNT"));
        assertEquals(2, completed.get());
    }

    /** Mirrors the dispatch task's finally block: bump the counter and the progress clock on completion. */
    private static CompletableFuture<Void> dispatch(long millis, AtomicInteger completed, AtomicLong lastProgress) {
        return CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                completed.incrementAndGet();
                lastProgress.set(System.nanoTime());
            }
        });
    }
}
