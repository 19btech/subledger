package com.reserv.dataloader.service;

import com.fyntrac.common.entity.Attributes;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;

@Service
@Slf4j
public class AttributeService {
    private final DataService dataService;

    @Autowired
    public AttributeService(DataService dataService) {
        this.dataService = dataService;
    }

    public Collection<Attributes> getReclassableAttributes() {
        Query query = new Query(Criteria.where("isReclassable").is(1));
        return dataService.fetchData(query, Attributes.class);
    }

    public Collection<Attributes> getAllAttributes() {
        return dataService.fetchAllData(Attributes.class);
    }
}
