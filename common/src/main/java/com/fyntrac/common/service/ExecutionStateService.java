package com.fyntrac.common.service;

import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.repository.MemcachedRepository;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

@Service
public class ExecutionStateService extends CacheBasedService<ExecutionState>{

    private String key;
    public ExecutionStateService(DataService<ExecutionState> dataService, MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
        this.key = String.format("%s-%s",this.getDataService().getTenantId(), "EXECUTION-STATE");
    }

    @Override
    public ExecutionState save(ExecutionState state) {
        return null;
    }

    public ExecutionState update(ExecutionState state) {

        ExecutionState executionState = this.dataService.save(state);
        this.memcachedRepository.putInCache(key, executionState);
        return executionState;
    }

    public ExecutionState getExecutionState() {
        // Fetch all ExecutionStates
        Collection<ExecutionState> states = this.fetchAll();
        if (!states.isEmpty()) {
            ExecutionState executionState = states.iterator().next(); // Get the first ExecutionState
            if(executionState == null) {
                ExecutionState.builder().executionDate(0).lastExecutionDate(0).build();
            }
            this.memcachedRepository.putInCache(this.key, executionState); // Cache the retrieved ExecutionState
            return executionState; // Return the retrieved ExecutionState
        }
        return null;
    }


    @Override
    public Collection<ExecutionState> fetchAll() {
        return  this.dataService.fetchAllData(ExecutionState.class);
    }

    public ExecutionState fetchLatest() {
        return this.dataService.fetchAllData(ExecutionState.class).stream()
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("No ExecutionState found"));
    }

    @Override
    public void loadIntoCache() throws ExecutionException, InterruptedException {

    }
}
