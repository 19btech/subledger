package com.reserv.dataloader.repository;

import com.fyntrac.common.entity.Aggregation;
import net.spy.memcached.MemcachedClient;
import org.springframework.batch.item.Chunk;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class AggregationMemcachedRepository extends MemcachedRepository {
    public AggregationMemcachedRepository(MemcachedClient memcachedClient) {
        super(memcachedClient);
    }

    public void putItemInCache(String tenantId, Chunk<? extends Aggregation> items) {
        for (Aggregation item : items) {
            this.putItemInCache(tenantId, item);
        }
    }

    private void putItemInCache(String tenantId, Aggregation aggregation) {
        String key = tenantId + aggregation.hashCode();
        List<String> metricList = null;
        if (this.ifExists(key)) {
            metricList = this.getFromCache(key, List.class);
            metricList.add(aggregation.getMetricName());
            this.replaceInCache(key, metricList);
        } else {
            metricList = new ArrayList<>(0);
            metricList.add(aggregation.getMetricName());
            this.putInCache(key, metricList);
        }


    }
}