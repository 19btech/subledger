package com.reserv.dataloader.service;

import com.fyntrac.common.enums.FileUploadActivityType;
import com.fyntrac.common.entity.ActivityLog;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Collection;


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
