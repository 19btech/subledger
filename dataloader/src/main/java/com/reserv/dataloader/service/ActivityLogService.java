package com.reserv.dataloader.service;

import  com.fyntrac.common.enums.AccountingRules;
import  com.fyntrac.common.enums.FileUploadActivityType;
import com.fyntrac.common.entity.ActivityLog;
import com.fyntrac.common.entity.Attributes;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import com.fyntrac.common.service.DataService;

import java.util.Collection;
import java.util.List;

import static org.springframework.data.mongodb.core.aggregation.SortOperation.*;

@Service
@Slf4j
public class ActivityLogService {

    private final DataService dataService;

    @Autowired
    public ActivityLogService(DataService dataService) {
        this.dataService = dataService;
    }

    public Collection<ActivityLog> getRecentLoad() {
        Sort sort = Sort.by(Sort.Direction.DESC, "endingTime");
        Query query = new Query(Criteria.where("activityType").is(FileUploadActivityType.TRANSACTION_ACTIVITY)).with(sort).limit(1);
        return dataService.fetchData(query, ActivityLog.class);
    }

}
