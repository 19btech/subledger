package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "DashboardConfiguration")
public class DashboardConfiguration implements Serializable {
    @Serial
    private static final long serialVersionUID = -7084344231702704492L;

    @Id
    private String id;
    private String widgetOneMetric;
    private String widgetTwoMetric;
    private String widgetThreeMetric;
    private String trendAnalysisGraphMetric;
    private String[] activityGraphMetrics;

    @Override
    public String toString() {
        return "{" +
                "\"id\": \"" + id + "\"," +
                "\"widgetOneMetric\": \"" + widgetOneMetric + "\"," +
                "\"widgetTwoMetric\": \"" + widgetTwoMetric + "\"," +
                "\"widgetThreeMetric\": \"" + widgetThreeMetric + "\"," +
                "\"trendAnalysisGraphMetric\": \"" + trendAnalysisGraphMetric + "\"," +
                "\"activityGraphMetrics\": " + Arrays.toString(activityGraphMetrics) +
                "}";
    }

}
