package com.fyntrac.common.service;

import com.fyntrac.common.entity.Model;
import com.fyntrac.common.entity.ModelConfig;
import com.fyntrac.common.enums.ModelStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Date;
import java.util.List;

@Service
@Slf4j
public class ModelService {

    private final DataService<Model> dataService;

    @Autowired
    public ModelService(DataService<Model> dataService){
        this.dataService = dataService;
    }

    public Model save(Model model) {
        return this.dataService.save(model);
    }

    public Model save(String modelName
            , String modelOrderId
            , String modelFileId
            , boolean isDeleted
            , ModelStatus modelStatus
            , Date uploadDate
            , String uploadedBy
            , ModelConfig modelConfig) {
        Model model = Model.builder()
                .modelName(modelName)
                .orderId(modelOrderId)
                .modelFileId(modelFileId)
                .isDeleted(isDeleted == Boolean.FALSE ? 0 : 1)
                .modelStatus(modelStatus)
                .uploadDate(uploadDate)
                .uploadedBy(uploadedBy)
                .modelConfig(modelConfig)
                .build();
        return this.dataService.save(model);
    }

    public Collection<Model> getModels() {
        Query query = new Query();
        Criteria criteria = Criteria.where("isDeleted").is(0);
        query.addCriteria(criteria);
        return this.dataService.fetchData(query, Model.class);
    }

    public boolean ifModelExists(String modelName) {
        Query query = new Query();
        Criteria criteria = Criteria.where("modelName").is(modelName);
        query.addCriteria(criteria);
        return this.dataService.fetchData(query, Model.class).isEmpty() ? Boolean.FALSE : Boolean.TRUE;
    }

    public Model getModel(String id) {
        Query query = new Query();
        Criteria criteria = Criteria.where("id").is(id);
        query.addCriteria(criteria);
        List<Model> modelList = this.dataService.fetchData(query, Model.class);
        if(modelList == null || modelList.isEmpty()) {
            return null;
        }
        return modelList.get(modelList.size()-1);
    }
}
