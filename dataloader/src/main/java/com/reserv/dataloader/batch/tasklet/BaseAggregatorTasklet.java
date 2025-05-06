package com.reserv.dataloader.batch.tasklet;

import com.fyntrac.common.entity.AggregationRequest;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.ExecutionStateService;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Autowired;
import com.fyntrac.common.service.SettingsService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public abstract class BaseAggregatorTasklet  {

    protected final MemcachedRepository memcachedRepository;
    protected final DataService dataService;
    protected  final SettingsService settingsService;
    protected final String tenantId;
    protected static final int CHUNK_SIZE = 10000;
    @Autowired
    protected JobParameters jobParametersAccessor;
    protected static final String KEY = "aggregation-key";
    protected final ExecutionStateService executionStateService;
    public BaseAggregatorTasklet(MemcachedRepository memcachedRepository
            , DataService dataService
                                 , SettingsService settingsService
                                 , ExecutionStateService executionStateService
            , String tenantId) {
        this.memcachedRepository = memcachedRepository;
        this.dataService = dataService;
        this.settingsService = settingsService;
        this.tenantId = tenantId;
        this.executionStateService = executionStateService;
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
