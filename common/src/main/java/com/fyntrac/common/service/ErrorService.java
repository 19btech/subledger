package com.fyntrac.common.service;

import com.fyntrac.common.entity.Errors;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.enums.ErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

@Service
@Slf4j
public class ErrorService {

    private final DataService<Errors> dataService;

    @Autowired
    public  ErrorService(DataService<Errors> dataService){
        this.dataService = dataService;
    }

    public Errors save(Errors error) {
        return this.dataService.save(error);
    }

    public List<Errors> getAll() {
        return this.dataService.fetchAllData(Errors.class);
    }

    public  List<Errors> getErrors() {
        // Define the query
        Query query = new Query();

        // Add criteria to filter by transactionName (list) and transactionDate
        query.addCriteria(Criteria.where("isWarning").is(Boolean.FALSE));
        // Execute the query
        return this.dataService.fetchData(query, Errors.class);
    }

    public List<Errors> getErrors(Date executionDate) {
        // Define the query
        Query query = new Query();

        // Add criteria to filter by transactionName (list) and transactionDate
        query.addCriteria(Criteria.where("executionDate").is(executionDate));

        // Execute the query
        return this.dataService.fetchData(query, Errors.class);
    }

    public List<Errors> getErrors(ErrorCode code) {
        // Define the query
        Query query = new Query();

        // Add criteria to filter by transactionName (list) and transactionDate
        query.addCriteria(Criteria.where("code").is(code));

        // Execute the query
        return this.dataService.fetchData(query, Errors.class);
    }

    public List<Errors> getWarnings() {
        // Define the query
        Query query = new Query();

        // Add criteria to filter by transactionName (list) and transactionDate
        query.addCriteria(Criteria.where("isWarning").is(Boolean.TRUE));

        // Execute the query
        return this.dataService.fetchData(query, Errors.class);
    }

    public List<Errors> getWarnings(Date executionDate) {
        // Define the query
        Query query = new Query();

        // Add criteria to filter by transactionName (list) and transactionDate
        query.addCriteria(Criteria.where("isWarning").is(Boolean.TRUE).and("executionDate").is(executionDate));

        // Execute the query
        return this.dataService.fetchData(query, Errors.class);
    }
}
