package com.reserv.dataloader.service;

import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.entity.ChartOfAccount;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class ChartOfAccountService {

    private final DataService dataService;

    @Autowired
    public ChartOfAccountService(DataService dataService) {
        this.dataService = dataService;
    }

    public List<ChartOfAccount> fetchDataByAttribute(String attributeName, Object attributeValue) {
        Query query = new Query();
        query.addCriteria(Criteria.where("attributes." + attributeName).exists(true).andOperator(Criteria.where("attributes." + attributeName).is(attributeValue)));
        return dataService.fetchData(query, ChartOfAccount.class);
    }

}
