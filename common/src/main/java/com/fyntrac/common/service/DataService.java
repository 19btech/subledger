package com.fyntrac.common.service;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.Sequence;
import com.fyntrac.common.enums.SequenceNames;
import com.fyntrac.common.utils.StringUtil;
import com.mongodb.client.result.UpdateResult;
import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.component.TenantDataSourceProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.*;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import com.mongodb.client.model.ReplaceOptions;
// MongoDB Document
import org.bson.Document;
import java.lang.reflect.Field;

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

    public <T> Collection<T> saveAll(List<T> entities, Class<T> klass) {
        String tenant = tenantContextHolder.getTenant();
        return this.saveAll(new HashSet<>(entities), tenant, klass);
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

    public <T> Collection<T> bulkSave(Set<T> entities, String tenantId, Class<T> klass) {
        MongoTemplate mongoTemplate = this.dataSourceProvider.getDataSource(tenantId);
        if (mongoTemplate == null) {
            throw new IllegalArgumentException("Invalid tenantId: " + tenantId);
        }

        BulkOperations bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, klass);

        for (T entity : entities) {
            Object id = this.getIdValue(entity);

            Document doc = new Document();
            mongoTemplate.getConverter().write(entity, doc);

            if (id != null) {
                // Update if ID exists
                Query query = new Query(Criteria.where("_id").is(id));
                Update update = Update.fromDocument(doc);
                bulkOps.upsert(query, update);
            } else {
                // Insert if no ID
                bulkOps.insert(entity);
            }
        }

        bulkOps.execute();
        return entities;
    }


    private <T> Object getIdValue(T entity) {
        for (Field field : entity.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(Id.class)) {
                field.setAccessible(true);
                try {
                    return field.get(entity);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to access @Id field", e);
                }
            }
        }
        return null;
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
        String tenant = TenantContextHolder.getTenant();
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
        this.truncateDatabase(tenant);
    }

    public void truncateDatabase(String tenant) {
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

    public long generateSequence(String tenantId, String sequenceName) {
        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenantId);

        Query query = new Query(Criteria.where("_id").is(sequenceName));
        Update update = new Update().inc("seq", 1);

        // Atomic and thread-safe: this creates the document if it doesn't exist and increments in one step
        FindAndModifyOptions options = FindAndModifyOptions.options()
                .returnNew(true)
                .upsert(true);

        Sequence sequence = mongoTemplate.findAndModify(query, update, options, Sequence.class);

        if (sequence == null) {
            throw new IllegalStateException("Failed to generate sequence for " + sequenceName);
        }

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


    /**
     * Approach 2: Dynamic / Raw Data
     * Returns a List of Documents (Map<String, Object>).
     * Best if you don't want to handle a POJO with null fields.
     *
     * @param collectionName The name of the collection in Mongo
     * @param criteria       Filtering logic
     * @param fields         The column names to retrieve
     * @return List of BSON Documents
     */
    public List<Document> findSelectedFieldsAsMap(String collectionName, Criteria criteria, List<String> fields) {
        var query = new Query(criteria);

        if (fields != null && !fields.isEmpty()) {
            fields.forEach(f -> query.fields().include(f));
        }

        return this.getMongoTemplate().find(query, Document.class, collectionName);
    }

    /**
     * Approach 2: Dynamic / Raw Data
     * Returns a List of Documents (Map<String, Object>).
     * Best if you don't want to handle a POJO with null fields.
     *
     * @param collectionName The name of the collection in Mongo
     * @param fields         The column names to retrieve
     * @return List of BSON Documents
     */
    public List<Document> findSelectedFieldsAsMap(String collectionName, List<String> fields) {
        var query = new Query();

        if (fields != null && !fields.isEmpty()) {
            fields.forEach(f -> query.fields().include(f));
        }

        return this.getMongoTemplate().findAll(Document.class, collectionName);
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
                    attributesWithTypes.add(RecordFactory.createDocumentAttribute(key + "." + mapKey, mapKey, mapDataType));
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

    /**
     * Get all collection names except system collections
     */
    public List<String> getUserCollectionNames() {
        return this.getMongoTemplate().getCollectionNames().stream()
                .filter(name -> !name.startsWith("system."))
                .filter(name -> !name.equals("table_definitions")) // Exclude metadata collection if needed
                .toList();
    }
}
