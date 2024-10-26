package com.reserv.dataloader.batch.tasklet;

import com.reserv.dataloader.aggregate.MetricLevelAggregator;
import  com.fyntrac.common.enums.AggregationRequestType;
import com.fyntrac.common.entity.AggregationRequest;
import com.fyntrac.common.entity.TransactionActivityList;
import com.fyntrac.common.repository.MemcachedRepository;
import com.reserv.dataloader.service.DataService;
import com.reserv.dataloader.service.AggregationRequestService;
import com.reserv.dataloader.service.SettingsService;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class MetricLevelAggregatorTasklet extends BaseAggregatorTasklet implements Tasklet {
    public MetricLevelAggregatorTasklet(MemcachedRepository memcachedRepository
            , DataService dataService
            , SettingsService settingsService
            , AggregationRequestService metricAggregationRequestService
            , String tenantId) {
       super(memcachedRepository
               , dataService
               , settingsService
               , metricAggregationRequestService
               , tenantId);
    }

    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        // Retrieve TransactionUpload object from Memcached
        if(this.tenantId == null) {
            return RepeatStatus.FINISHED;
        }

        AggregationRequest aggregationRequest = this.metricAggregationRequestService.getAggregationRequest(AggregationRequestType.METRIC_LEVEL_AGG);

        if (aggregationRequest != null && !aggregationRequest.isInprogress() && !aggregationRequest.isAggregationComplete()) {
            // Read TransactionActivity objects from Memcached
            this.aggregateTransactionActivities(aggregationRequest);
        }


        return RepeatStatus.FINISHED;
    }

    @Override
    public void aggregateTransactionActivities(AggregationRequest aggregationRequest) throws InterruptedException, ExecutionException {

        String key = aggregationRequest.getKey();

        TransactionActivityList transactionActivities = this.memcachedRepository.getFromCache(key, TransactionActivityList.class);

        List<List<String>>chunks = chunkList(transactionActivities.get(), CHUNK_SIZE);

        ExecutorService executor = Executors.newFixedThreadPool(chunks.size());

        List<Future<List<String>>> futures = new ArrayList<>();
        for (List<String> chunk : chunks) {
            futures.add(executor.submit(new AggregationTask(new MetricLevelAggregator(this.memcachedRepository, this.dataService, this.settingsService,this.tenantId)
                                                            ,chunk)));
        }

        executor.shutdown();
    }
}