package  com.reserv.dataloader.config;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.Tenant;
import com.reserv.dataloader.service.TenantService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import com.fyntrac.common.config.TenantDatasourceConfig;
import com.fyntrac.common.service.SequenceGenerator;
import java.util.List;

@Configuration
public class TenantDatabaseConfig  extends TenantDatasourceConfig{
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
