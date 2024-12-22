package com.fyntrac.common.entity;

import com.fyntrac.common.enums.AggregationLevel;
import com.fyntrac.common.dto.record.Records;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.poi.hssf.record.Record;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;

@Data
@NoArgsConstructor
public class ModelConfig  implements Serializable {
    @Serial
    private static final long serialVersionUID = -7678540694499281451L;

    private Records.TransactionNameRecord [] transactions;
    private Records.MetricNameRecord[] metrics;
    private AggregationLevel aggregationLevel;
    private boolean isCurrentVersion;
    private boolean isLastOpenVersion;
    private boolean isFirstVersion;

    @Override
    public String toString() {
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{");

        jsonBuilder.append("\"transactions\":").append(Arrays.toString(transactions)).append(",");
        jsonBuilder.append("\"metrics\":").append(Arrays.toString(metrics)).append(",");
        jsonBuilder.append("\"aggregationLevel\":\"").append(aggregationLevel).append("\",");
        jsonBuilder.append("\"isCurrentVersion\":\"").append(isCurrentVersion).append("\",");
        jsonBuilder.append("\"isLastOpenVersion\":\"").append(isLastOpenVersion).append("\",");
        jsonBuilder.append("\"isFirstVersion\":\"").append(isFirstVersion).append("\",");

        jsonBuilder.append("}");

        return jsonBuilder.toString()
                .replaceAll("(?<=\\[|,|:|\\{)\\s*\"(.*?)\"\\s*", "\"$1\"") // Remove spaces around strings
                .replaceAll("(?<=\\[|,|:|\\{)\\s*(\\w+)\\s*(?=\\]|,|:|\\})", "$1"); // Remove spaces around non-strings
    }
}
