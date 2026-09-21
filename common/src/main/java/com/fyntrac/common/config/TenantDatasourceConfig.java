package com.fyntrac.common.config;

import com.fyntrac.common.component.MongoIndexEnsurer;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.Tenant;
import com.fyntrac.common.service.SequenceGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

import java.util.List;

@Slf4j
public class TenantDatasourceConfig {
    private final TenantDataSourceProvider tenantDataSourceProvider;
    private final MappingMongoConverter mappingMongoConverter;
    private final SequenceGenerator sequenceGenerator;

    // Field-injected rather than added to the constructor so the two existing subclasses
    // (TenantDatabaseConfig, gl's DatasourceService) don't need their constructor calls updated —
    // Spring still autowires inherited @Autowired fields on whichever concrete subclass is the
    // actual bean.
    @Autowired
    private MongoIndexEnsurer mongoIndexEnsurer;

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

    // The Mongo Java driver's default maxPoolSize is 100 — never overridden here before, so every
    // per-tenant MongoClient was silently capped at 100 connections. Confirmed directly (live
    // currentOp() check): a single dataloader replica's TNT002 pool sat pinned at exactly 100/100
    // in-use connections during event generation, since a page's instrument groups are processed
    // concurrently (up to fyntrac.chunk.size at once, unbounded) and each does several Mongo
    // queries — once the pool's 100 slots were all checked out, every further operation queued
    // inside the driver waiting for one to free up, adding latency on top of the query cost
    // itself. Raised so a free connection is available for the next queued operation instead of
    // that operation blocking on pool exhaustion; paired with bounding page-level concurrency
    // itself (see ExcelModelService.maxConcurrentInstrumentGroups) so the pool isn't just pushed
    // to a higher ceiling that gets hit again as data grows.
    @Value("${spring.data.mongodb.tenant-max-pool-size:300}")
    private int tenantMaxPoolSize;

    @Autowired
    public TenantDatasourceConfig(MappingMongoConverter mappingMongoConverter
            , TenantDataSourceProvider tenantDataSourceProvider
            , SequenceGenerator sequenceGenerator) {
        this.mappingMongoConverter = mappingMongoConverter;
        this.tenantDataSourceProvider = tenantDataSourceProvider;
        this.sequenceGenerator = sequenceGenerator;
    }

    public void configureTenantDatabases(List<Tenant> tenants) {
        for (Tenant tenant : tenants) {
            this.configureTenantDatabases(tenant);
        }
    }

    public void configureTenantDatabases(Tenant tenant) {
        this.configureTenantDatabases(tenant.getTenantCode());
    }

    public void configureTenantDatabases(String tenantCode) {
        if(tenantDataSourceProvider.getDataSource(tenantCode) == null) {
            String connectionURI = getConnectionURI(tenantCode);
            MongoDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(connectionURI);
            MongoTemplate template = new MongoTemplate(factory, mappingMongoConverter);
            tenantDataSourceProvider.addDataSource(tenantCode, template);
            if (sequenceGenerator != null) {
                sequenceGenerator.generateAllSequences(template, tenantCode);
            }
            // See MongoIndexEnsurer: auto-index-creation never reaches this dynamically-built
            // per-tenant template, so every @Indexed/@CompoundIndex annotation is otherwise inert
            // for tenant data. This covers first-time provisioning; DataService.truncateDatabase
            // re-runs the same thing after dropping collections, since that drops their indexes too.
            if (mongoIndexEnsurer != null) {
                mongoIndexEnsurer.ensureIndexes(tenantCode, template);
            } else {
                log.warn("[Tenant={}] MongoIndexEnsurer not available — skipping index creation for this tenant.",
                        tenantCode);
            }
        }
    }

    private String getConnectionURI(String tenantName) {
        return String.format("mongodb://%s:%s@%s:%s/%s?authSource=%s&readPreference=primaryPreferred&directConnection=true&maxPoolSize=%d",
                this.userName,
                this.pswd,
                this.dbHost,
                this.dbPort,
                tenantName,
                this.authDB,
                this.tenantMaxPoolSize);
    }
}
