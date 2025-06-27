package com.reserv.dataloader.batch.reader;

import com.fyntrac.common.component.InstrumentReplaySet;
import com.fyntrac.common.dto.record.Records;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.Iterator;
import java.util.List;

@StepScope
public class InstrumentReplayRecordReader implements ItemReader<Records.InstrumentReplayRecord>, StepExecutionListener {

    @Value("${fyntrac.chunk.size}")
    private int chunkSize;

    @Autowired
    private InstrumentReplaySet instrumentReplaySet;

    private String tenantId;

    private Long jobId;

    private int currentChunkIndex = 0;
    private Iterator<Records.InstrumentReplayRecord> currentBatchIterator = null;

    public InstrumentReplayRecordReader(String tenantId, Long jobId,InstrumentReplaySet instrumentReplaySet) {
        this.tenantId = tenantId;
        this.jobId = jobId;
        this.instrumentReplaySet = instrumentReplaySet;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        // Reset state when the step starts
        this.currentChunkIndex = 0;
        this.currentBatchIterator = null;
    }

    @Override
    public Records.InstrumentReplayRecord read() {
        while (true) {
            if (currentBatchIterator == null || !currentBatchIterator.hasNext()) {
                List<Records.InstrumentReplayRecord> nextBatch =
                        instrumentReplaySet.readChunk(tenantId, jobId, chunkSize, currentChunkIndex);

                if (nextBatch == null || nextBatch.isEmpty()) {
                    return null; // ✅ Proper end of reading
                }

                currentBatchIterator = nextBatch.iterator();
                currentChunkIndex++;
            }

            if (currentBatchIterator.hasNext()) {
                return currentBatchIterator.next();
            }

            // If it reached here, something is wrong, so loop again
        }
    }

}
