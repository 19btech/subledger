package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.InstrumentReplayState;
import org.springframework.batch.item.ItemProcessor;

public class InstrumentReplayStateProcessor implements ItemProcessor<Records.InstrumentReplayRecord, InstrumentReplayState> {
    @Override
    public InstrumentReplayState process(Records.InstrumentReplayRecord record) {
        return InstrumentReplayState.builder().instrumentId(record.instrumentId())
                .attributeId(record.attributeId())
                .minEffectiveDate(record.effectiveDate())
                .maxPostingDate(record.postingDate())
                .build();
    }

}
