package com.fyntrac.common.entity;

import com.fyntrac.common.enums.FileUploadActivityType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.sql.Blob;
import java.time.LocalDateTime;
import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "ActivityLog")
public class ActivityLog {
    @Id
    private String id;
    private String jobName;
    private long jobId;
    private String uploadFilePath;
    private long activityUploadId;
    private LocalDateTime startingTime;
    private LocalDateTime endingTime;
    private FileUploadActivityType activityType;
    private String activityStatus;
    private String activityDetails;

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"activityUploadId\":\"").append(activityUploadId).append("\",");
        json.append("\"activityType\":\"").append(activityType).append("\",");
        json.append("\"startingTime\":\"").append(startingTime).append("\",");
        json.append("\"endingTime\":\"").append(endingTime).append("\",");
        json.append("\"activityStatus\":\"").append(activityStatus).append("\",");
        json.append("\"activityDetails\":\"").append(activityDetails).append("\",");
        json.append("}");
        return json.toString();
    }
}
