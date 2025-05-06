package com.fyntrac.common.service;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.repository.MemcachedRepository;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Service
public class ExecutionStateService extends CacheBasedService<ExecutionState>{

    private String key;
    public ExecutionStateService(DataService<ExecutionState> dataService, MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
        this.key = String.format("%s-%s",this.getDataService().getTenantId(), "EXECUTION-STATE");
    }

    @Override
    public void save(ExecutionState state) {

    }

    public ExecutionState update(ExecutionState state) {

        ExecutionState executionState = this.dataService.save(state);
        this.memcachedRepository.putInCache(key, executionState);
        return executionState;
    }

    public Optional<ExecutionState> getExecutionState() {
        // Check cache first
        ExecutionState cached = (ExecutionState) this.memcachedRepository.getFromCache(key);
        if (cached != null) {
            return Optional.of(cached);
        }

        // Not in cache - fetch from source
        Collection<ExecutionState> states = this.fetchAll();
        Optional<ExecutionState> executionState = states.stream().findFirst();

        // Cache if found
        if (executionState.isPresent()) {
            this.memcachedRepository.putInCache(key, executionState.get());
        }

        return executionState;
    }

    @Override
    public Collection<ExecutionState> fetchAll() {
        return  this.dataService.fetchAllData(ExecutionState.class);
    }

    @Override
    public void loadIntoCache() throws ExecutionException, InterruptedException {

    }
}
