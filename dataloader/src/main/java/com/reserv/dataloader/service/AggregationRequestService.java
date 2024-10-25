package com.reserv.dataloader.service;

import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.enums.AggregationRequestType;
import com.fyntrac.common.entity.AggregationQueue;
import com.fyntrac.common.entity.AggregationRequest;
import com.reserv.dataloader.repository.MemcachedRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AggregationRequestService {

    @Autowired
    private MemcachedRepository memcachedRepository;
    @Autowired
    private TenantContextHolder tenantContextHolder;

    public void save(AggregationRequest aggregationRequest, AggregationRequestType identity) {
        String key = this.tenantContextHolder.getTenant() + identity;
        if(!this.memcachedRepository.ifExists(key)) {
            AggregationQueue queue = new AggregationQueue();
            this.memcachedRepository.putInCache(key, queue, 43200);
        }
        AggregationQueue queue = this.memcachedRepository.getFromCache(key, AggregationQueue.class);
        queue.add(aggregationRequest);
        this.memcachedRepository.replaceInCache(key, queue);
    }

    public AggregationRequest getAggregationRequest(AggregationRequestType identity) {
        if(this.tenantContextHolder.getTenant() == null) {
            return null;
        }
        String key = this.tenantContextHolder.getTenant() + identity;
        AggregationQueue queue = this.memcachedRepository.getFromCache(key, AggregationQueue.class);
        AggregationRequest aggregationRequest = queue.poll();
        this.memcachedRepository.replaceInCache(key, queue);
        return aggregationRequest;
    }

    public boolean isAggregationRequestExists(AggregationRequestType identity) {
        if(this.tenantContextHolder.getTenant() == null) {
            return false;
        }
        String key = this.tenantContextHolder.getTenant() + identity;
        return this.memcachedRepository.ifExists(key);
    }
}


