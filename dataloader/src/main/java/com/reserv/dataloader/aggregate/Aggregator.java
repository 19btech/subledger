package com.reserv.dataloader.aggregate;

import com.fyntrac.common.entity.TransactionActivity;

import java.util.List;

public interface Aggregator {
    void aggregate(List<String> activities);
    void aggregate(TransactionActivity activity);
    void cleanup();
    List<String> getCleanupList();
}
