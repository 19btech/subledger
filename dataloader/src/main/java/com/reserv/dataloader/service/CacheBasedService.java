package com.reserv.dataloader.service;

import com.reserv.dataloader.repository.MemcachedRepository;

public abstract class CacheBasedService<T> extends BaseService<T>{

    public CacheBasedService(DataService<T> dataService, MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
    }

    public abstract void loadIntoCache();
}
