package com.reserv.dataloader.batch.reader;

import com.fyntrac.common.component.InstrumentReplayQueue;
import com.fyntrac.common.component.InstrumentReplaySet;
import com.fyntrac.common.dto.record.Records;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InstrumentReplayQueueReader implements ItemReader<Records.InstrumentReplayRecord> {
    private final List<Records.InstrumentReplayRecord> allRecords;
    private int index = 0;

    public InstrumentReplayQueueReader(String tenantId, Long jobId, InstrumentReplayQueue queue) {
        this.allRecords = new ArrayList<>();
        Records.InstrumentReplayRecord record;
        while ((record = queue.poll(tenantId, jobId)) != null) {
            allRecords.add(record);
        }
    }

    @Override
    public Records.InstrumentReplayRecord read() {
        if (index < allRecords.size()) {
            return allRecords.get(index++);
        } else {
            return null;
        }
    }
}

