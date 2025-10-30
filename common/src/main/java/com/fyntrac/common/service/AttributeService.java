package com.fyntrac.common.service;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AttributeService extends CacheBasedService<Attributes> {

    @Autowired
    public AttributeService(DataService dataService, MemcachedRepository memcachedRepository) {
        super(dataService, memcachedRepository);
    }

    public Collection<Attributes> getReclassableAttributes() {
        Query query = new Query(Criteria.where("isReclassable").is(1));
        return dataService.fetchData(query, Attributes.class);
    }

    public Collection<Attributes> getReclassableAttributes(String tenantId) {
        String key = Key.reclassAttributes(tenantId);
        CacheList<Attributes> attributes = new CacheList<>();
        if (!(this.memcachedRepository.ifExists(key))) {
            Query query = new Query(Criteria.where("isReclassable").is(1));
            Collection<Attributes> reclassAttributes = dataService.fetchData(query, tenantId, Attributes.class);
            attributes.addAll(reclassAttributes);
            this.memcachedRepository.putInCache(key, attributes);
        }

        attributes = this.memcachedRepository.getFromCache(key, CacheList.class);
        return attributes.getList();
    }

    public Collection<Attributes> getAllAttributes() {
        return dataService.fetchAllData(Attributes.class);
    }

    public List<String> getAttributeNames() {
        return this.dataService.getMongoTemplate().query(Attributes.class)  // Replace
                // Metric.class with your actual class
                .distinct("attributeName")          // Specify the field name
                .as(String.class)                // Specify the return type
                .all();
    }

    public Collection<Option> getAttributeNameOptions() {
        return this.getAttributeNames()
                .stream()
                .map(s -> Option.builder().label(s).value(s).build())
                .collect(Collectors.toList());
    }

    @Override
    public void loadIntoCache() throws ExecutionException, InterruptedException {

    }

    @Override
    public Attributes save(Attributes attributes) {
        return this.dataService.save(attributes);

    }

    @Override
    public Collection<Attributes> fetchAll() {
        return List.of();
    }
}
