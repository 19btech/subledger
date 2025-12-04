package com.reserv.dataloader.batch.writer;

import org.bson.Document;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

public class DynamicMongoWriter implements ItemWriter<Document> {

    private final MongoTemplate mongoTemplate;
    private final String collectionName;

    public DynamicMongoWriter(MongoTemplate mongoTemplate, String collectionName) {
        this.mongoTemplate = mongoTemplate;
        this.collectionName = collectionName;
    }

    @Override
    public void write(Chunk<? extends Document> chunk) throws Exception {
        for (Document doc : chunk) {
            // This inserts into the dynamic collection name (e.g., "Customers", "Products")
            mongoTemplate.save(doc, collectionName);
        }
    }
}