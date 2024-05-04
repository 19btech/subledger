package com.reserv.dataloader.service;

import com.reserv.dataloader.component.TenantContextHolder;
import com.reserv.dataloader.component.TenantDataSourceProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DataService {

    private final TenantDataSourceProvider dataSourceProvider;
    private final TenantContextHolder tenantContextHolder;

    @Autowired
    public DataService(TenantDataSourceProvider dataSourceProvider, TenantContextHolder tenantContextHolder) {
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
    }

    public void save(Object data) {
        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        if (mongoTemplate != null) {
            mongoTemplate.save(data);
        } else {
            throw new IllegalArgumentException("Invalid tenantId: " + tenant);
        }
    }

    public <T> List<T> fetchData(Query query, Class<T> documentClass) {
        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        return mongoTemplate.find(query, documentClass);
    }

    public <T> List<T> fetchAllData(Class<T> documentClass) {
        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        return mongoTemplate.findAll(documentClass);
    }

    public <T> List<T> findByColumns(Class<T> documentClass, String... columns) {
        Query query = new Query();
        for (String column : columns) {
            query.fields().include(column); // Include each specified column in the projection
        }

        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        List<T> results = mongoTemplate.find(query, documentClass);

        return results;
    }
}