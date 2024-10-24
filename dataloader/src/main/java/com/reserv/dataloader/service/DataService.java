package com.reserv.dataloader.service;

import com.mongodb.client.result.UpdateResult;
import com.reserv.dataloader.config.TenantContextHolder;
import com.reserv.dataloader.component.TenantDataSourceProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

@Service
public class DataService<T> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final TenantContextHolder tenantContextHolder;

    @Autowired
    public DataService(TenantDataSourceProvider dataSourceProvider, TenantContextHolder tenantContextHolder) {
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
    }

    public T save(T data) {
        String tenant = tenantContextHolder.getTenant();
       return this.save(data, tenant);
    }

    public Object saveObject(Object data) {
        String tenant = tenantContextHolder.getTenant();
        return this.saveObject(data, tenant);
    }

    public T save(T data, String tenantId) {
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);
        if (mongoTemplate != null) {
            return mongoTemplate.save(data);
        } else {
            throw new IllegalArgumentException("Invalid tenantId: " + tenantId);
        }
    }

    public Object saveObject(Object data, String tenantId) {
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);
        if (mongoTemplate != null) {
            return mongoTemplate.save(data);
        } else {
            throw new IllegalArgumentException("Invalid tenantId: " + tenantId);
        }
    }

    public <T> Collection<T> saveAll(Set<T> entities, Class<T> klass) {
        String tenant = tenantContextHolder.getTenant();
        return this.saveAll(entities, tenant, klass);
    }

    public <T> Collection<T> saveAll(Set<T> entities,String tenantId, Class<T> klass) {
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);
        if (mongoTemplate != null) {
            TimeZone.setDefault(TimeZone.getTimeZone("IST"));
            return mongoTemplate.insert(entities, klass);
        } else {
            throw new IllegalArgumentException("Invalid tenantId: " + tenantId);
        }
    }

    public List<T> fetchData(Query query, Class<T> documentClass) {
        String tenant = tenantContextHolder.getTenant();
        return this.fetchData(query, tenant, documentClass);
    }

    public List<T> fetchData(Query query, String tenantId, Class<T> documentClass) {
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);
        return mongoTemplate.find(query, documentClass);
    }

    public List<T> fetchAllData(String tenantId, Class<T> documentClass) {
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);
        return mongoTemplate.findAll(documentClass);
    }

    public List<T> fetchAllData(Class<T> documentClass) {
        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        return mongoTemplate.findAll(documentClass);
    }

    public List<T> findByColumns(Class<T> documentClass, String... columns) {
        Query query = new Query();
        for (String column : columns) {
            query.fields().include(column); // Include each specified column in the projection
        }

        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);

        return mongoTemplate.find(query, documentClass);
    }

    public UpdateResult update(Query query, Update update, Class<T> documentClass) {
        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        return mongoTemplate.updateMulti(query, update, documentClass);
    }

    public void truncateDatabase() {
        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        Set<String> collectionNames = mongoTemplate.getCollectionNames();
        for (String collectionName : collectionNames) {
            mongoTemplate.getDb().getCollection(collectionName).drop();
        }
    }

    public void truncateCollection(Class<T> collection) {
        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        String collectionName = mongoTemplate.getCollectionName(collection);
        mongoTemplate.getCollection(collectionName).drop();
    }

    public String getTenantId() {
        return tenantContextHolder.getTenant();
    }

    public void setTenantId(String tenantId) {
        tenantContextHolder.setTenant(tenantId);
    }

    public String getAccountingPeriodKey() {
        return  this.getTenantId() + "-" + "AP";
    }
}