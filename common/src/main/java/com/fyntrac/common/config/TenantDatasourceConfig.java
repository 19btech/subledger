package com.fyntrac.common.config;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.Tenant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

import java.util.List;

public class TenantDatasourceConfig {
    private final TenantDataSourceProvider tenantDataSourceProvider;
    private final MappingMongoConverter mappingMongoConverter;

    @Value("${spring.data.mongodb.uri}")
    private String defaultMongoUri;

    @Value("${spring.data.mongodb.host}")
    private String dbHost;
    @Value("${spring.data.mongodb.port}")
    private String dbPort;
    @Value("${spring.data.mongodb.database}")
    private String defaultDB;
    @Value("${spring.data.mongodb.username}")
    private String userName;
    @Value("${spring.data.mongodb.password}")
    private String pswd;
    @Value("${spring.data.mongodb.authentication-database}")
    private String authDB;

    @Autowired
    public TenantDatasourceConfig(MappingMongoConverter mappingMongoConverter, TenantDataSourceProvider tenantDataSourceProvider) {
        this.mappingMongoConverter = mappingMongoConverter;
        this.tenantDataSourceProvider = tenantDataSourceProvider;
    }

    public void configureTenantDatabases(List<Tenant> tenants) {
        for (Tenant tenant : tenants) {
            this.configureTenantDatabases(tenant);
        }
    }

    public void configureTenantDatabases(Tenant tenant) {
        String connectionURI = getConnectionURI(tenant.getName());
        MongoDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(connectionURI);
        MongoTemplate template = new MongoTemplate(factory, mappingMongoConverter);
        tenantDataSourceProvider.addDataSource(tenant.getName(), template);
    }

    private String getConnectionURI(String tenantName) {
        return String.format("mongodb://%s:%s@%s:%s/%s?authSource=%s&readPreference=primaryPreferred&directConnection=true",
                this.userName,
                this.pswd,
                this.dbHost,
                this.dbPort,
                tenantName,
                this.authDB);
    }
}