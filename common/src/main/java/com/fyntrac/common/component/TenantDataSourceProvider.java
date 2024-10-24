package com.fyntrac.common.component;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class TenantDataSourceProvider {

    private final Map<String, MongoTemplate> dataSourceMap = new HashMap<>();

    public void addDataSource(String tenantId, MongoTemplate mongoTemplate) {
        dataSourceMap.put(tenantId, mongoTemplate);
    }

    public MongoTemplate getDataSource(String tenantId) {
        return dataSourceMap.get(tenantId);
    }
}