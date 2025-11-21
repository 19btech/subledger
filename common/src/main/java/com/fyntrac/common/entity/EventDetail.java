package com.fyntrac.common.entity;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.enums.SourceType;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class EventDetail {
    String sourceTable;
    SourceType sourceType;
    String sourceKey;
    Map<String, Map<String, Object>> values;
}
