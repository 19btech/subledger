package com.reserv.dataloader.config;

import com.reserv.dataloader.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.Tenant;
import com.reserv.dataloader.service.TenantService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

import java.util.List;

@Configuration
public class TenantDatabaseConfig {
    private final TenantDataSourceProvider tenantDataSourceProvider;
    private final MappingMongoConverter mappingMongoConverter;
    private final TenantService tenantService;

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
    public TenantDatabaseConfig(TenantService tenantService
                                , TenantDataSourceProvider tenantDataSourceProvider
                                , MappingMongoConverter mappingMongoConverter) {
        this.tenantDataSourceProvider = tenantDataSourceProvider;
        this.mappingMongoConverter = mappingMongoConverter;
        this.tenantService = tenantService;
    }

    @PostConstruct
    public void configureTenantDatabases() {
        List<Tenant> tenants = tenantService.getAllTenants();
        for (Tenant tenant : tenants) {
            String connectionURI = getConnectionURI(tenant.getName());
            MongoDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(connectionURI);
            MongoTemplate template = new MongoTemplate(factory, mappingMongoConverter);
            tenantDataSourceProvider.addDataSource(tenant.getName(), template);
        }
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
