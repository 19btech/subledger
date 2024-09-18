package com.reserv.dataloader.service;

import com.reserv.dataloader.entity.Aggregation;
import com.reserv.dataloader.repository.MemcachedRepository;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collection;


@Getter
public abstract class BaseService<T> {
    protected final DataService<T> dataService;
    protected final MemcachedRepository memcachedRepository;

    @Autowired
    public BaseService(DataService<T> dataService
                      , MemcachedRepository memcachedRepository) {
        this.dataService = dataService;
        this.memcachedRepository = memcachedRepository;
    }

    public abstract void save(T t);

    public abstract Collection<T> fetchAll();
}
