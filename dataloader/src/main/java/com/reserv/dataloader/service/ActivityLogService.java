package com.reserv.dataloader.service;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.enums.ActivityType;
import com.reserv.dataloader.entity.ActivityLog;
import com.fyntrac.common.enums.FileUploadActivityType;
import com.fyntrac.common.service.DataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.WordUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ActivityLogService {

    private final DataService dataService;

    @Autowired
    public ActivityLogService(DataService dataService) {
        this.dataService = dataService;
    }

    public Collection<ActivityLog> getRecentLoad() {
        Sort sort = Sort.by(Sort.Direction.DESC, "uploadId");
        Query query = new Query(
                new Criteria().orOperator(
                        Criteria.where("activityType").is(FileUploadActivityType.TRANSACTION_ACTIVITY),
                        Criteria.where("activityType").is(FileUploadActivityType.INSTRUMENT_ATTRIBUTE),
                        Criteria.where("activityType").is(FileUploadActivityType.CUSTOM_TABLE)
                )
        ).with(sort).limit(11);
        return dataService.fetchData(query, ActivityLog.class);
    }

    public Collection<Records.ActivityLogRecord> getRecentActivityLog() {

        Collection<ActivityLog> logs = this.getRecentLoad();
        Map<Long, List<ActivityLog>> logsByUploadId = logs.stream()
                .collect(Collectors.groupingBy(
                        ActivityLog::getUploadId,
                        () -> new TreeMap<>(Comparator.reverseOrder()), // Forces Descending Key Order
                        Collectors.toList()
                ));

        Collection<Records.ActivityLogRecord> activityLogs = new ArrayList<>();

        logsByUploadId.forEach((uploadId, activityList) -> {

            // 1. Initialize trackers to null
            LocalDateTime minStartTime = null;
            LocalDateTime maxEndTime = null;
            String jobStatus = "COMPLETE"; // Default assumption

            List<Records.ActivityLogDetailRecord> details = new ArrayList<>();
            Set<String> jobNameSet = HashSet.newHashSet(0);
            for (ActivityLog log : activityList) {



                String tableName = WordUtils.capitalizeFully(log.getTableName()); // Safety net
                if(log.getActivityType() == FileUploadActivityType.CUSTOM_TABLE) {
                    jobNameSet.add("Custom Activity");
                }else {
                    jobNameSet.add("Standard Activity");
                }

                // Status Logic: If ANY part failed, mark the whole job as the failed status
                if (log.getActivityStatus() != null && !log.getActivityStatus().equalsIgnoreCase("COMPLETE")) {
                    jobStatus = log.getActivityStatus();
                }

                // 2. Logic: Keep the Earliest Start Time (MIN)
                if (minStartTime == null || (log.getStartingTime() != null && log.getStartingTime().isBefore(minStartTime))) {
                    minStartTime = log.getStartingTime();
                }

                // 3. Logic: Keep the Latest End Time (MAX)
                if (maxEndTime == null || (log.getEndingTime() != null && log.getEndingTime().isAfter(maxEndTime))) {
                    maxEndTime = log.getEndingTime();
                }

                // Create Detail Record
                Records.ActivityLogDetailRecord detail = RecordFactory.createActivityLogDetailRecord(
                        tableName,
                        log.getRecordsRead(),
                        log.getRecordsWritten(),
                        log.getRecordsSkipped(),
                        log.getStartingTime(),
                        log.getEndingTime(),
                        log.getErrorMessage()
                );
                details.add(detail);
            }

            StringBuffer jobName = new StringBuffer();
            int counter = 0;
            for(String name : jobNameSet) {
                if(counter > 0){
                    jobName.append(",");
                }
                jobName.append(name);
                counter++;
            }
            // 4. Create the final Summary Record using the calculated Min/Max times
            Records.ActivityLogRecord logRecord = RecordFactory.createActivityLogRecord(
                    uploadId,
                    jobName.toString(),
                    minStartTime,
                    maxEndTime,
                    jobStatus,
                    details
            );

            activityLogs.add(logRecord);
        });

        return activityLogs;
    }
}
