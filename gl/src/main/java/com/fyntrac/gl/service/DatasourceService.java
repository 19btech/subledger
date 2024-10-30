package com.fyntrac.gl.service;

import com.fyntrac.common.component.TenantDataSourceProvider;
import org.springframework.stereotype.Service;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import com.fyntrac.common.service.SequenceGenerator;

@Service
public class DatasourceService extends com.fyntrac.common.config.TenantDatasourceConfig {
    public DatasourceService(MappingMongoConverter mappingMongoConverter
            , TenantDataSourceProvider tenantDataSourceProvider, SequenceGenerator sequenceGenerator) {
        super(mappingMongoConverter, tenantDataSourceProvider, sequenceGenerator);
    }

    public void addDatasource(String tenant) {
        super.configureTenantDatabases(tenant);
    }
}
