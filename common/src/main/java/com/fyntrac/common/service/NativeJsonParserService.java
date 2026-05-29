package com.fyntrac.common.service;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;
import org.springframework.stereotype.Service;
import java.util.Map;

public class NativeJsonParserService {

    public static Records.JobResultResponseRecord parseJobResultSafely(String jsonPayload) {
        try {
            // 1. Parse the raw string into a standard Java Map
            Map<String, Object> jsonMap = JsonParserFactory.getJsonParser().parseMap(jsonPayload);

            // 2. Extract fields using native Map safety nets

            // Handle Long safely (Spring might parse small numbers as Integer or Long)
            Number jobIdNum = (Number) jsonMap.get("jobId");
            Long jobId = (jobIdNum != null) ? jobIdNum.longValue() : null;

            // Handle String safely
            String status = (String) jsonMap.get("status");

            // FAIL-SAFE: If successCount is missing, default to 0 natively
            Number successCountNum = (Number) jsonMap.get("successCount");
            Integer successCount = (successCountNum != null) ? successCountNum.intValue() : 0;

            // 3. Map to your internal DTO/Record structure
            return RecordFactory.createJobResultResponseRecord(jobId, status, successCount);

        } catch (Exception e) {
            // Catches invalid/malformed JSON strings entirely
            throw new RuntimeException("Catastrophic string parsing error, baseline payload malformed", e);
        }
    }
}