package com.reserv.dataloader.batch.tasklet;

import com.fyntrac.common.service.TransactionActivityService;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class InstrumentActivityStateTasklet implements Tasklet {

    private final TransactionActivityService activityService;

    @Autowired
    public InstrumentActivityStateTasklet(TransactionActivityService activityService) {
        this.activityService = activityService;
    }
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        try{
            this.activityService.updateInstrumentActivityState();
        }catch (Exception exception){
            log.error(String.format("Failed: updateInstrumentActivityState %s", StringUtil.getStackTrace(exception)));
        }
        return RepeatStatus.FINISHED;
    }
}
