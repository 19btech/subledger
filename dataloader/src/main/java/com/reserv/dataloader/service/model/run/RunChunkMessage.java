package com.reserv.dataloader.service.model.run;

/**
 * One chunk of a distributed DSL run: the active instruments with
 * {@code fromExclusive < instrumentId <= toInclusive} (a null bound = unbounded), for one posting date.
 * Published once per chunk by the coordinator; any dataloader pod may take it. The chunk's state lives
 * in {@code ExecutionRunChunk} (see {@link DistributedRunService}) — this message only says which one.
 */
public record RunChunkMessage(String tenantId,
                              String runId,
                              int postingDate,
                              int seq,
                              String fromExclusive,
                              String toInclusive,
                              boolean sharedReferenceEventsExist) {

    public String chunkId() {
        return chunkId(runId, seq);
    }

    public static String chunkId(String runId, int seq) {
        return runId + ":" + seq;
    }
}
