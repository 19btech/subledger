package com.reserv.dataloader.batch.tasklet;

import com.reserv.dataloader.aggregate.Aggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class AggregationTask implements Callable<List<String>> {
    private final List<String> chunk;
    private final Aggregator aggregator;
    private static final int NUM_BUCKETS = 10000;
    public AggregationTask(Aggregator aggregator
            , List<String> chunk) {
        this.chunk = chunk;
        this.aggregator = aggregator;
    }

    @Override
    public List<String> call() {
        List<String> transctionActivityKeys = new ArrayList<>();
        Map<String, Integer> bucketCounts = new HashMap<>();
        for (int i = 0; i < NUM_BUCKETS; i++) {
            bucketCounts.put("bucket" + i, 0);
        }
        aggregator.aggregate(chunk);
        return aggregator.getCleanupList();
    }
}