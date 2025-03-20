package com.fyntrac.common.service;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.Sequence;
import com.fyntrac.common.enums.SequenceNames;
import com.mongodb.client.result.UpdateResult;
import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.component.TenantDataSourceProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
// MongoDB Document
import org.bson.Document;

import java.util.*;

@Service
@Slf4j
public class DataService<T> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final TenantContextHolder tenantContextHolder;

    @Autowired
    public DataService(TenantDataSourceProvider dataSourceProvider, TenantContextHolder tenantContextHolder) {
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
    }

    public MongoTemplate getMongoTemplate() {
        return this.dataSourceProvider.getDataSource(this.tenantContextHolder.getTenant());
    }

    public MongoTemplate getMongoTemplate(String tenantId) {
        return this.dataSourceProvider.getDataSource(tenantId);
    }

    public T save(T data) {
        String tenant = tenantContextHolder.getTenant();
        return this.save(data, tenant);
    }

    public Object saveObject(Object data) {
        String tenant = tenantContextHolder.getTenant();
        return this.saveObject(data, tenant);
    }

    public T save(T data, String tenantId) {
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);
        if (mongoTemplate != null) {
            return mongoTemplate.save(data);
        } else {
            throw new IllegalArgumentException("Invalid tenantId: " + tenantId);
        }
    }

    public Object saveObject(Object data, String tenantId) {
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);
        if (mongoTemplate != null) {
            return mongoTemplate.save(data);
        } else {
            throw new IllegalArgumentException("Invalid tenantId: " + tenantId);
        }
    }

    public <T> Collection<T> saveAll(Set<T> entities, Class<T> klass) {
        String tenant = tenantContextHolder.getTenant();
        return this.saveAll(entities, tenant, klass);
    }

    public <T> Collection<T> saveAll(Set<T> entities,String tenantId, Class<T> klass) {
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);
        if (mongoTemplate != null) {
            TimeZone.setDefault(TimeZone.getTimeZone("IST"));
            return mongoTemplate.insert(entities, klass);
        } else {
            throw new IllegalArgumentException("Invalid tenantId: " + tenantId);
        }
    }

    public <T> Collection<T> saveAll(Collection<T> entities,String tenantId, Class<T> klass) {
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);
        if (mongoTemplate != null) {
            TimeZone.setDefault(TimeZone.getTimeZone("IST"));
            return mongoTemplate.insert(entities, klass);
        } else {
            throw new IllegalArgumentException("Invalid tenantId: " + tenantId);
        }
    }
    public List<T> fetchData(Query query, Class<T> documentClass) {
        String tenant = tenantContextHolder.getTenant();
        return this.fetchData(query, tenant, documentClass);
    }

    public List<T> fetchData(Query query, String tenantId, Class<T> documentClass) {
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);
        return mongoTemplate.find(query, documentClass);
    }

    public List<T> fetchAllData(String tenantId, Class<T> documentClass) {
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);
        return mongoTemplate.findAll(documentClass);
    }

    public List<T> fetchAllData(Class<T> documentClass) {
        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        return mongoTemplate.findAll(documentClass);
    }

    public List<T> findByColumns(Class<T> documentClass, String... columns) {
        Query query = new Query();
        for (String column : columns) {
            query.fields().include(column); // Include each specified column in the projection
        }

        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);

        return mongoTemplate.find(query, documentClass);
    }

    public UpdateResult update(Query query, Update update, Class<T> documentClass) {
        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        return mongoTemplate.updateMulti(query, update, documentClass);
    }

    public void truncateDatabase() {
        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        Set<String> collectionNames = mongoTemplate.getCollectionNames();
        for (String collectionName : collectionNames) {
            mongoTemplate.getDb().getCollection(collectionName).drop();
        }
    }

    public void truncateCollection(Class<T> collection) {
        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        String collectionName = mongoTemplate.getCollectionName(collection);
        mongoTemplate.getCollection(collectionName).drop();
    }

    public String getTenantId() {
        return tenantContextHolder.getTenant();
    }

    public void setTenantId(String tenantId) {
        tenantContextHolder.setTenant(tenantId);
    }

    public String getAccountingPeriodKey() {
        return  this.getTenantId() + "-" + "AP";
    }

    public T findOne(Query query, Class<T> t) {
        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        return mongoTemplate.findOne(query,  t);
    }

    public T findOne(Query query, String tenantId, Class<T> t) {
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);
        // Fetch the single unique document
        return mongoTemplate.findOne(query, t);
    }

    // Strategy interface for custom mapping
    public interface BulkWriteStrategy<T> {
        Object getId(T entity);
        Document createUpdateDocument(T entity);
    }

    /**
     * Upserts a HashSet of objects into the specified collection.
     *
     * @param objectSet The set of objects to be upserted.
     * @param clazz     The class type of the objects.
     * @param <T>       The type of the objects.
     * @param identifierKey The unique key used to identify existing records.
     * @throws Exception Throws exception if bulk operation fails.
     */
    public <T> void upsertHashSet(HashSet<T> objectSet, Class<T> clazz, String identifierKey) throws Exception {
        if (objectSet == null || objectSet.isEmpty()) {
            return;
        }

        try {
            // Prepare bulk operations
            String tenant = tenantContextHolder.getTenant();
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
            BulkOperations bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, clazz);

            for (T object : objectSet) {
                // Extract the identifier key's value using reflection
                Object identifierValue = getFieldValue(object, identifierKey);

                if (identifierValue == null) {
                    throw new IllegalArgumentException("Identifier key value cannot be null");
                }

                // Create a query to match the identifier
                Query query = new Query(Criteria.where(identifierKey).is(identifierValue));

                // Create an update object to set all fields of the object
                Update update = new Update();
                for (java.lang.reflect.Field field : clazz.getDeclaredFields()) {
                    field.setAccessible(true); // Allow access to private fields
                    update.set(field.getName(), field.get(object));
                }

                // Add upsert operation to bulkOps
                bulkOps.upsert(query, update);
            }

            // Execute bulk operation
            bulkOps.execute();
        } catch (Exception e) {
            // Log and rethrow the exception
            System.err.println("Error during bulk upsert: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Extracts the value of a field from an object using reflection.
     *
     * @param object The object.
     * @param fieldName The field name.
     * @return The value of the field.
     * @throws IllegalAccessException If the field cannot be accessed.
     */
    private Object getFieldValue(Object object, String fieldName) throws IllegalAccessException {
        try {
            java.lang.reflect.Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(object);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("Field '" + fieldName + "' not found in class " + object.getClass().getName());
        }
    }

    public long generateSequence(String sequenceName) {

        String tenant = tenantContextHolder.getTenant();
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);

       return generateSequence(tenant, sequenceName);
    }

    public long generateSequence(String tenantid, String sequenceName) {

        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantid);

        // Check if the sequence exists
        Query query = new Query(Criteria.where("id").is(sequenceName));
        Sequence sequence = mongoTemplate.findOne(query, Sequence.class);

        if (sequence == null) {
            // If the sequence does not exist, create it with an initial value of 0
            sequence = new Sequence();
            sequence.setId(sequenceName);
            sequence.setSeq(0);
            mongoTemplate.insert(sequence);
        }

        // Increment the sequence and return the new value
        Update update = new Update().inc("seq", 1);
        sequence = mongoTemplate.findAndModify(query, update, FindAndModifyOptions.options().returnNew(true), Sequence.class);

        return sequence.getSeq();
    }
    public void generateAllSequences(MongoTemplate mongoTemplate, String tenant) {
        if(mongoTemplate == null){
            log.error("MongoTemplate is null for tenant[{}]", tenant);
        }
        this.generateSequence(SequenceNames.INSTRUMENTATTRIBUTEVERSIONID.name());
    }

    public void copyData(String attributes, Criteria criteria, String targetCollection, String sourceCollection, Class<T> klass) {
        String tenant = tenantContextHolder.getTenant();

        this.copyData(tenant, criteria, targetCollection, sourceCollection, klass, attributes);
    }

    public void copyData(String tenant
            , Criteria criteria
            , String targetCollection
            , String sourceCollection
            , Class<T> klass
            , String ... attributes) {
        // Define the conditions for filtering
        // Criteria criteria = Criteria.where("someField").is("someValue"); // Adjust your filter criteria

        // Define the aggregation pipeline
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(criteria), // Apply your filter criteria here
                // Aggregation.project("attributeId", "instrumentId", "transactionName", "transactionDate", "periodId", "glAccountNumber", "glAccountName", "glAccountType", "glAccountSubType", "debitAmount", "creditAmount", "isReclass", "batchId", "attributes"),
                Aggregation.project(attributes),
                Aggregation.out(targetCollection) // This will create or replace the target collection
        );

        // Execute the aggregation
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
        mongoTemplate.aggregate(aggregation, sourceCollection, klass);

        log.info("Data copied successfully!");
    }

    public List<Records.DocumentAttribute> getAttributesWithTypes(String collectionName) {
        List<Records.DocumentAttribute> attributesWithTypes = new ArrayList<>();

        // Fetch a single document from the collection
        Document document = this.getMongoTemplate().findOne(new org.springframework.data.mongodb.core.query.Query(), Document.class, collectionName);

        if (document != null) {
            extractAttributesWithTypes(document, attributesWithTypes);
        }

        return attributesWithTypes;
    }

    private void extractAttributesWithTypes(Document document, List<Records.DocumentAttribute> attributesWithTypes) {
        for (String key : document.keySet()) {
            // Skip _id and _class fields
            if ("_id".equals(key) || "_class".equals(key)) {
                continue;
            }

            Object value = document.get(key);
            String dataType = value.getClass().getSimpleName();
            String attributeAlias = convertToHungarianNotation(key);

            if (value instanceof Document) {
                // If the value is a nested document, recurse into it
                extractAttributesWithTypes((Document) value, attributesWithTypes);
            } else if (value instanceof Map) {
                // If the value is a Map, iterate through its entries
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    String mapKey = entry.getKey().toString();
                    Object mapValue = entry.getValue();
                    String mapDataType = mapValue.getClass().getSimpleName();
                    String mapAttributeAlias = convertToHungarianNotation(mapKey);
                    attributesWithTypes.add(RecordFactory.createDocumentAttribute(key + "." + mapKey, mapAttributeAlias, mapDataType));
                }
            } else {
                // Otherwise, just add the attribute and its type
                attributesWithTypes.add(RecordFactory.createDocumentAttribute(key, attributeAlias, dataType));
            }
        }
    }

    private String convertToHungarianNotation(String attributeName) {
        // Convert attribute name to Hungarian notation
        StringBuilder alias = new StringBuilder();
        String[] parts = attributeName.split("(?=\\p{Upper})"); // Split before uppercase letters
        for (String part : parts) {
            if (alias.length() > 0) {
                alias.append(" ");
            }
            alias.append(part);
        }

        // Capitalize the first letter of the alias
        if (alias.length() > 0) {
            alias.setCharAt(0, Character.toUpperCase(alias.charAt(0)));
        }

        return alias.toString();
    }
}
