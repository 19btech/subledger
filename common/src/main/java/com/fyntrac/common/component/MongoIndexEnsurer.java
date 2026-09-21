package com.fyntrac.common.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver.IndexDefinitionHolder;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * spring.data.mongodb.auto-index-creation only wires index creation up against the single
 * auto-configured MongoTemplate Spring Boot builds at startup — it never runs against a
 * MongoTemplate built for a tenant's own separate database (see TenantDatasourceConfig), so
 * every @Indexed/@CompoundIndex annotation across every entity was silently inert for tenant
 * data (a live check found nothing but the default _id index on collections with 16k-90k+
 * documents).
 * <p>
 * This resolves those same annotations from the shared mapping context and ensures them
 * directly against a given tenant's template — mirrors what Spring Boot's own
 * MongoPersistentEntityIndexCreator does for the auto-configured template, just pointed at the
 * right database. Called both when a tenant's database is first provisioned
 * (TenantDatasourceConfig) and after anything that drops collections, since dropping a
 * collection drops its indexes too (DataService.truncateDatabase — which
 * ExcelTestDriver.setUp() and SettingsService both call on every run).
 */
@Component
@Slf4j
public class MongoIndexEnsurer {

    private final MappingMongoConverter mappingMongoConverter;

    @Autowired
    public MongoIndexEnsurer(MappingMongoConverter mappingMongoConverter) {
        this.mappingMongoConverter = mappingMongoConverter;
    }

    /**
     * Ensures every entity's declared @Indexed/@CompoundIndex indexes exist on the given
     * tenant's MongoTemplate. ensureIndex is a no-op if the index already exists, so this is
     * safe (and cheap enough) to call every time, not just once per tenant.
     */
    public void ensureIndexes(String tenantCode, MongoTemplate template) {
        MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext =
                mappingMongoConverter.getMappingContext();
        if (!(mappingContext instanceof MongoMappingContext mongoMappingContext)) {
            log.warn("[Tenant={}] MappingMongoConverter's mapping context is not a MongoMappingContext " +
                    "({}) — skipping index creation for this tenant.",
                    tenantCode, mappingContext.getClass().getName());
            return;
        }

        MongoPersistentEntityIndexResolver resolver = new MongoPersistentEntityIndexResolver(mongoMappingContext);
        for (MongoPersistentEntity<?> entity : mongoMappingContext.getPersistentEntities()) {
            List<IndexDefinitionHolder> indexDefinitions;
            try {
                indexDefinitions = resolver.resolveIndexForEntity(entity);
            } catch (Exception e) {
                // The mapping context also registers non-root types it encounters as embedded
                // fields/payloads elsewhere (e.g. plain DTO records never annotated @Document) —
                // resolveIndexForEntity rejects those as "not a collection root". Never block
                // tenant provisioning/truncation over one such entity — log and move on, same
                // "don't crash over one tenant/one entity" spirit as TenantDatabaseConfig's
                // @PostConstruct loop.
                log.warn("[Tenant={}] Failed to resolve indexes for entity {}: {}",
                        tenantCode, entity.getType().getName(), e.getMessage());
                continue;
            }
            for (IndexDefinitionHolder indexDefinition : indexDefinitions) {
                try {
                    template.indexOps(indexDefinition.getCollection()).ensureIndex(indexDefinition);
                } catch (Exception e) {
                    // Never block tenant provisioning/truncation over one bad index definition —
                    // log and move on, same "don't crash over one tenant/one entity" spirit as
                    // TenantDatabaseConfig's @PostConstruct loop.
                    log.warn("[Tenant={}] Failed to ensure index {} on collection {}: {}",
                            tenantCode, indexDefinition.getIndexKeys(), indexDefinition.getCollection(), e.getMessage());
                }
            }
        }
    }
}
