package com.fyntrac.common.config;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.Tenant;
import com.fyntrac.common.service.SequenceGenerator;
import com.fyntrac.common.service.TenantService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

import java.util.List;

@Configuration
public class TenantDatabaseConfig extends TenantDatasourceConfig{
    private final TenantService tenantService;

    @Autowired
    public TenantDatabaseConfig(TenantService tenantService
                                , TenantDataSourceProvider tenantDataSourceProvider
                                , MappingMongoConverter mappingMongoConverter
                                , SequenceGenerator sequenceGenerator) {
        super(mappingMongoConverter, tenantDataSourceProvider, sequenceGenerator);
        this.tenantService = tenantService;
    }

    @PostConstruct
    public void configureTenantDatabases() {
        List<Tenant> tenants = tenantService.getAllTenants();
        this.configureTenantDatabases(tenants);
    }


}
