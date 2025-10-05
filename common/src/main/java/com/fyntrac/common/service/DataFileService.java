package com.fyntrac.common.service;

import com.fyntrac.common.entity.DataFiles;
import com.fyntrac.common.enums.DataActivityType;
import com.fyntrac.common.enums.DataFileType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
public class DataFileService {

    private final DataService<DataFiles> dataService;

    @Autowired
    public DataFileService(DataService<DataFiles> dataService) {
        this.dataService = dataService;
    }

    /**
     * Fetch all DataFiles
     */
    public List<DataFiles> findAll() {
        return dataService.getMongoTemplate().findAll(DataFiles.class);
    }

    /**
     * Find by ID
     */
    public Optional<DataFiles> findById(String id) {
        return Optional.ofNullable(dataService.getMongoTemplate().findById(id, DataFiles.class));
    }

    /**
     * Find by fileName
     */
    public List<DataFiles> findByFileName(String fileName) {
        Query query = new Query(Criteria.where("fileName").is(fileName));
        return dataService.getMongoTemplate().find(query, DataFiles.class);
    }

    /**
     * Find by contentType
     */
    public List<DataFiles> findByContentType(String contentType) {
        Query query = new Query(Criteria.where("contentType").is(contentType));
        return dataService.getMongoTemplate().find(query, DataFiles.class);
    }

    /**
     * Find by DataFileType
     */
    public List<DataFiles> findByDataFileType(DataFileType fileType) {
        Query query = new Query(Criteria.where("dataFileType").is(fileType.name()));
        return dataService.getMongoTemplate().find(query, DataFiles.class);
    }

    /**
     * Find by DataActivityType
     */
    public List<DataFiles> findByDataActivityType(DataActivityType activityType) {
        Query query = new Query(Criteria.where("dataActivityType").is(activityType.name()));
        return dataService.getMongoTemplate().find(query, DataFiles.class);
    }

    /**
     * Find by upload date range
     */
    public List<DataFiles> findByUploadedBetween(Instant start, Instant end) {
        Query query = new Query(Criteria.where("uploadedAt").gte(start).lte(end));
        return dataService.getMongoTemplate().find(query, DataFiles.class);
    }

    /**
     * Save or update a DataFile
     */
    public DataFiles save(DataFiles dataFile) {
        return dataService.getMongoTemplate().save(dataFile);
    }

    /**
     * Delete by ID
     */
    public void deleteById(String id) {
        Query query = new Query(Criteria.where("id").is(id));
        dataService.getMongoTemplate().remove(query, DataFiles.class);
    }

    /**
     * Delete by fileName
     */
    public void deleteByFileName(String fileName) {
        Query query = new Query(Criteria.where("fileName").is(fileName));
        dataService.getMongoTemplate().remove(query, DataFiles.class);
    }
}

