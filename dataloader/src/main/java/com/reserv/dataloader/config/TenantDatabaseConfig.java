package  com.fyntrac.common.config;

import com.fyntrac.common.component.TenantDataSourceProvider;
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
import com.fyntrac.common.config.TenantDatasourceConfig;
import java.util.List;

@Configuration
public class TenantDatabaseConfig  extends TenantDatasourceConfig{
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
        super(mappingMongoConverter, tenantDataSourceProvider);
        this.tenantService = tenantService;
    }

    @PostConstruct
    public void configureTenantDatabases() {
        List<Tenant> tenants = tenantService.getAllTenants();
        this.configureTenantDatabases(tenants);
    }


}
