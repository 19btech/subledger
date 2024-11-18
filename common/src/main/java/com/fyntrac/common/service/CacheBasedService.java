package com.fyntrac.common.service;

import com.fyntrac.common.repository.MemcachedRepository;

import java.util.concurrent.ExecutionException;

public abstract class CacheBasedService<T> extends BaseService<T>{

    public CacheBasedService(DataService<T> dataService, MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
    }

    public abstract void loadIntoCache() throws ExecutionException, InterruptedException;

}
