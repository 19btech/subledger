package com.fyntrac.model.service;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.entity.Model;
import com.fyntrac.common.entity.ModelFile;
import com.fyntrac.common.enums.ModelStatus;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.InstrumentAttributeService;
import com.fyntrac.common.service.TransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class ModelDataService {

    private final TransactionService transactionService;
    private final InstrumentAttributeService instrumentAttributeService;
    private final DataService<Model> dataService;
    private final DataService<ModelFile> modelFileDataService;

    @Autowired
    public ModelDataService(TransactionService transactionService
    , InstrumentAttributeService instrumentAttributeService
    , DataService<Model> dataService
    , DataService<ModelFile> modelFileDataService) {
        this.transactionService = transactionService;
        this.instrumentAttributeService = instrumentAttributeService;
        this.dataService = dataService;
        this.modelFileDataService = modelFileDataService;
    }

    public List<Model> getActiveModels() {
        Query query = new Query();
        query.addCriteria(Criteria.where("isDeleted").is(0).and("modelStatus").is(ModelStatus.ACTIVE));

        // Add sorting to the query (e.g., sort by uploadDate in descending order)
        query.with(Sort.by(Sort.Direction.ASC, "orderId"));
        // Execute the aggregation
        return this.dataService.fetchData(query, Model.class);
    }

    public ModelFile getModelFile(String fileId) {
        try {
            Query query = new Query();
            query.addCriteria(Criteria.where("_id").is(fileId));
            // Execute the query
            return this.modelFileDataService.findOne(query, ModelFile.class);
        } catch (DataAccessException e) {
            // Handle data access exceptions (e.g., database connectivity issues)
            System.err.println("Data access error while retrieving model file: " + e.getMessage());
            // You can also log the error using a logging framework
            throw e; // or throw a custom exception
        } catch (Exception e) {
            // Handle any other exceptions
            System.err.println("An error occurred while retrieving model file: " + e.getMessage());
            throw e; // or throw a custom exception
        }
    }

}
