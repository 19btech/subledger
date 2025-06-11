package com.reserv.dataloader.batch.tasklet;

import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.aggregation.AggregationService;
import com.fyntrac.common.service.aggregation.InstrumentLevelAggregationService;
import com.reserv.dataloader.aggregate.InstrumentLevelAggregator;
import  com.fyntrac.common.enums.AggregationRequestType;
import com.fyntrac.common.entity.AggregationRequest;
import com.fyntrac.common.entity.TransactionActivityList;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.DataService;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import com.fyntrac.common.service.SettingsService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class InstrumentLevelAggregatorTasklet extends BaseAggregatorTasklet implements Tasklet {
    private final InstrumentLevelAggregationService instrumentLevelAggregationService;
    public InstrumentLevelAggregatorTasklet(MemcachedRepository memcachedRepository
            , DataService dataService
            , SettingsService settingsService
                                            , ExecutionStateService executionStateService
                                            , AccountingPeriodService accountingPeriodService
                                            , AggregationService aggregationService
                                            , InstrumentLevelAggregationService instrumentLevelAggregationService
            , String tenantId) {
        super(memcachedRepository
                , dataService
                , settingsService
                ,executionStateService
                , accountingPeriodService
                , aggregationService
                , tenantId);
        this.instrumentLevelAggregationService = instrumentLevelAggregationService;
    }

    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        // Retrieve TransactionUpload object from Memcached
        if(this.tenantId == null) {
            return RepeatStatus.FINISHED;
        }


        String key = contribution.getStepExecution().getJobParameters().getString(this.KEY);
        int executionDate = contribution.getStepExecution().getJobParameters().getLong("execution-date").intValue();
        int previousMaxPostingDate = contribution.getStepExecution().getJobParameters().getLong("previousMaxPostingDate").intValue();

        AggregationRequest aggregationRequest = AggregationRequest.builder()
                .isAggregationComplete(Boolean.FALSE)
                .isInprogress(Boolean.FALSE)
                .tenantId(this.dataService.getTenantId())
                .requestType(AggregationRequestType.INSTRUMENT_LEVEL_AGG)
                .postingDate(executionDate)
                .lastPostingDate(previousMaxPostingDate)
                .key(key).build();


            // Read TransactionActivity objects from Memcached
            this.aggregateTransactionActivities(aggregationRequest);


        return RepeatStatus.FINISHED;
    }
    @Override
    public void aggregateTransactionActivities(AggregationRequest aggregationRequest) throws InterruptedException, ExecutionException {

        String key = aggregationRequest.getKey();

        TransactionActivityList transactionActivities = this.memcachedRepository.getFromCache(key, TransactionActivityList.class);

        List<List<String>>chunks = chunkList(transactionActivities.get(), CHUNK_SIZE);

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        List<Future<List<String>>> futures = new ArrayList<>();
        for (List<String> chunk : chunks) {
            futures.add(executor.submit(new AggregationTask(new InstrumentLevelAggregator(this.memcachedRepository, this.dataService, this.settingsService, this.accountingPeriodService, this.aggregationService,this.instrumentLevelAggregationService,aggregationRequest,this.tenantId)
                    ,chunk)));
        }
        executor.shutdown();
    }
}
