package com.fyntrac.common.config;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class MultiTenantMongoDbFactory implements MongoDatabaseFactory {

    private final Map<String, MongoDatabaseFactory> tenantFactories = new ConcurrentHashMap<>();

    @Value("${spring.data.mongodb.uri:}")
    private String defaultMongoUri;

    @Value("${spring.data.mongodb.host:localhost}")
    private String dbHost;

    @Value("${spring.data.mongodb.port:27017}")
    private String dbPort;

    @Value("${spring.data.mongodb.database:default}")
    private String defaultDB;

    @Value("${spring.data.mongodb.username:}")
    private String userName;

    @Value("${spring.data.mongodb.password:}")
    private String pswd;

    @Value("${spring.data.mongodb.authentication-database:admin}")
    private String authDB;

    @Override
    public MongoDatabase getMongoDatabase() throws org.springframework.dao.DataAccessException {
        return getMongoDatabase(getDatabaseName());
    }

    @Override
    public MongoDatabase getMongoDatabase(String dbName) throws org.springframework.dao.DataAccessException {
        return getTenantFactory().getMongoDatabase(dbName);
    }

    @Override
    public PersistenceExceptionTranslator getExceptionTranslator() {
        return getTenantFactory().getExceptionTranslator();
    }

    @Override
    public ClientSession getSession(ClientSessionOptions options) {
        return getTenantFactory().getSession(options);
    }

    @Override
    public MongoDatabaseFactory withSession(ClientSession session) {
        return getTenantFactory().withSession(session);
    }


    @Override
    public boolean isTransactionActive() {
        return getTenantFactory().isTransactionActive();
    }

    public String getDatabaseName() {
        String tenantId = TenantContextHolder.getTenant();
        if (tenantId == null) {
            // Use default database if no tenant is set
            return defaultDB;
        }
        return tenantId; // Use tenant ID as database name
    }

    private MongoDatabaseFactory getTenantFactory() {
        String tenantId = TenantContextHolder.getTenant();
        if (tenantId == null || tenantId.isEmpty()) {
            // If no tenant is set, use default database
            tenantId = defaultDB;
        }

        return tenantFactories.computeIfAbsent(tenantId, this::createTenantFactory);
    }

    private MongoDatabaseFactory createTenantFactory(String tenantId) {
        String connectionString = buildConnectionString(tenantId);
        return new SimpleMongoClientDatabaseFactory(connectionString);
    }

    private String buildConnectionString(String databaseName) {
        // If default MongoDB URI is provided, use it with the database name
        if (defaultMongoUri != null && !defaultMongoUri.trim().isEmpty()) {
            // Extract the base URI without database and append the tenant database
            String baseUri = defaultMongoUri.substring(0, defaultMongoUri.lastIndexOf("/"));
            return baseUri + "/" + databaseName + getConnectionParameters();
        }

        // Build connection string from individual properties
        if (userName != null && !userName.trim().isEmpty() &&
                pswd != null && !pswd.trim().isEmpty()) {
            // With authentication
            return String.format("mongodb://%s:%s@%s:%s/%s?authSource=%s&readPreference=primaryPreferred&directConnection=true&ssl=false",
                    encodeURIComponent(userName),
                    encodeURIComponent(pswd),
                    dbHost,
                    dbPort,
                    databaseName,
                    authDB);
        } else {
            // Without authentication
            return String.format("mongodb://%s:%s/%s?readPreference=primaryPreferred&directConnection=true&ssl=false",
                    dbHost,
                    dbPort,
                    databaseName);
        }
    }

    private String getConnectionParameters() {
        // Extract and append connection parameters from the default URI if available
        if (defaultMongoUri != null && defaultMongoUri.contains("?")) {
            String params = defaultMongoUri.substring(defaultMongoUri.indexOf("?"));
            return params + "&directConnection=true";
        }
        return "?readPreference=primaryPreferred&directConnection=true&ssl=false";
    }

    private String encodeURIComponent(String component) {
        if (component == null) return "";
        try {
            return java.net.URLEncoder.encode(component, java.nio.charset.StandardCharsets.UTF_8)
                    .replaceAll("\\+", "%20");
        } catch (Exception e) {
            return component;
        }
    }

    // Clean up resources when needed
    public void cleanup() {
        tenantFactories.clear();
    }

    // Method to manually add a tenant factory (useful for dynamic tenant registration)
    public void addTenantFactory(String tenantId, String connectionString) {
        tenantFactories.put(tenantId, new SimpleMongoClientDatabaseFactory(connectionString));
    }

    // Method to remove a tenant factory
    public void removeTenantFactory(String tenantId) {
        tenantFactories.remove(tenantId);
    }

    // Check if a tenant factory exists
    public boolean hasTenantFactory(String tenantId) {
        return tenantFactories.containsKey(tenantId);
    }
}