package com.reserv.dataloader.batch.tasklet;

import com.fyntrac.common.entity.AggregationRequest;
import com.fyntrac.common.repository.MemcachedRepository;
import com.reserv.dataloader.service.DataService;
import com.reserv.dataloader.service.AggregationRequestService;
import com.reserv.dataloader.service.SettingsService;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public abstract class BaseAggregatorTasklet  {

    protected final MemcachedRepository memcachedRepository;
    protected final AggregationRequestService metricAggregationRequestService;
    protected final DataService dataService;
    protected  final SettingsService settingsService;
    protected final String tenantId;
    protected static final int CHUNK_SIZE = 10000;
    @Autowired
    protected JobParameters jobParametersAccessor;
    public BaseAggregatorTasklet(MemcachedRepository memcachedRepository
            , DataService dataService
                                 , SettingsService settingsService
            , AggregationRequestService metricAggregationRequestService
            , String tenantId) {
        this.memcachedRepository = memcachedRepository;
        this.metricAggregationRequestService = metricAggregationRequestService;
        this.dataService = dataService;
        this.settingsService = settingsService;
        this.tenantId = tenantId;
    }

    protected List<List<String>> chunkList(List<String> list, int chunkSize) {
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < list.size(); i += chunkSize) {
            chunks.add(list.subList(i, Math.min(i + chunkSize, list.size())));
        }
        return chunks;
    }

    public abstract void aggregateTransactionActivities(AggregationRequest aggregationRequest) throws InterruptedException, ExecutionException;
}
