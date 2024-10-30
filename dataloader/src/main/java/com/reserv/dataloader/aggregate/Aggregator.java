package com.reserv.dataloader.aggregate;

import com.fyntrac.common.entity.TransactionActivity;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface Aggregator {
    void aggregate(List<String> activities);
    void aggregate(TransactionActivity activity);
    void cleanup() throws ExecutionException, InterruptedException;
    List<String> getCleanupList();
}
