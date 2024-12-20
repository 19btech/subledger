package com.fyntrac.common.service;

import com.fyntrac.common.entity.Model;
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
public class ModelConfigurationService {

    private final DataService<Model> dataService;

    @Autowired
    public ModelConfigurationService(DataService<Model> dataService){
        this.dataService = dataService;
    }

    public Model save(Model model) {
        return this.dataService.save(model);
    }

    public Model save(String modelName, String modelOrderId, String modelFileId, boolean isDeleted, ModelStatus modelStatus, Date uploadDate, String uploadedBy) {
        Model model = Model.builder()
                .modelName(modelName)
                .orderId(modelOrderId)
                .modelFileId(modelFileId)
                .isDeleted(isDeleted == Boolean.FALSE ? 0 : 1)
                .modelStatus(modelStatus)
                .uploadDate(uploadDate)
                .uploadedBy(uploadedBy)
                .build();
        return this.dataService.save(model);
    }

    public Collection<Model> getModels() {
        Query query = new Query();
        Criteria criteria = Criteria.where("isDeleted").is(0);
        query.addCriteria(criteria);
        return this.dataService.fetchData(query, Model.class);
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
