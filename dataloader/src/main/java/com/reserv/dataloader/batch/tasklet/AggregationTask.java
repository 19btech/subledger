package com.reserv.dataloader.batch.tasklet;

import com.fyntrac.common.entity.TransactionActivity;
import com.reserv.dataloader.aggregate.Aggregator;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class AggregationTask implements Callable<List<TransactionActivity>> {
    private final List<TransactionActivity> chunk;
    private final Aggregator aggregator;

    @Value("${fyntrac.chunk.size}")
    private int NUM_BUCKETS;

    public AggregationTask(Aggregator aggregator
            , List<TransactionActivity> chunk) {
        this.chunk = chunk;
        this.aggregator = aggregator;
    }

    @Override
    public List<TransactionActivity> call() {
        List<String> transctionActivityKeys = new ArrayList<>();
        Map<String, Integer> bucketCounts = new HashMap<>();
        for (int i = 0; i < NUM_BUCKETS; i++) {
            bucketCounts.put("bucket" + i, 0);
        }
        aggregator.aggregate(chunk);
        return aggregator.getCleanupList();
    }
}