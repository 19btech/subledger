package com.fyntrac.common.config;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.Tenant;
import com.fyntrac.common.service.SequenceGenerator;
import com.fyntrac.common.service.TenantService;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

import java.util.List;

@Configuration
@Slf4j
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
        for (Tenant tenant : tenants) {
            try {
                this.configureTenantDatabases(tenant);
            } catch (Exception e) {
                // NEVER crash the whole application because one tenant has bad data.
                // Log prominently so ops can fix the data, then re-register the tenant
                // via the /refresh/schema endpoint without a full restart.
                log.error("[Tenant={}] Failed to configure tenant database during startup. " +
                          "This tenant will be unavailable until the data issue is resolved. " +
                          "Check for duplicate @Indexed fields (e.g., Transactions.name).",
                          tenant.getTenantCode(), e);
            }
        }
    }


}
