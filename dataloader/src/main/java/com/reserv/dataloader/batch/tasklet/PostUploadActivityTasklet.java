package com.reserv.dataloader.batch.tasklet;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.Key;
import com.reserv.dataloader.pulsar.producer.GeneralLedgerMessageProducer;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;

public class PostUploadActivityTasklet implements Tasklet {

    private final String runIdKey = "run.id";
    @Autowired
    GeneralLedgerMessageProducer generalLedgerMessageProducer;

    @Autowired
    MemcachedRepository memcachedRepository;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        try{
            this.bookGL(contribution);
        }catch (Exception e) {

            return RepeatStatus.FINISHED;
        }

        return RepeatStatus.FINISHED;
    }

    public void bookGL(StepContribution contribution) {
         String tenantId = contribution.getStepExecution().getJobParameters().getString("tenantId");
        long jobId = contribution.getStepExecution().getJobParameters().getLong("jobid");
        Records.GeneralLedgerMessageRecord glRec = RecordFactory.createGeneralLedgerMessageRecord(tenantId, jobId);
        generalLedgerMessageProducer.bookTempGL(glRec);
    }
}
