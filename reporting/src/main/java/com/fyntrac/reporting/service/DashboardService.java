package com.fyntrac.reporting.service;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.SettingsService;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DashboardService {
    private final SettingsService settingsService;
    private final ExecutionStateService executionStateService;
    private final DataService<MetricLevelLtd> dataService;
    public DashboardService(SettingsService settingsService,
                            ExecutionStateService executionStateService,
                            DataService<MetricLevelLtd> dataService) {
        this.settingsService = settingsService;
        this.executionStateService = executionStateService;
        this.dataService = dataService;
    }

    private Query buildQuery(String metricName, Integer postingDate) {
        return new Query(Criteria.where("metricName").is(metricName.toUpperCase())
                .and("postingDate").is(postingDate));
    }

    public List<MetricLevelLtd> getWidgetData(){
        Settings settings = settingsService.fetch();
        DashboardConfiguration dashboardConfiguration = settings.getDashboardConfiguration();
        ExecutionState executionState = executionStateService.getExecutionState();
        Integer executionDate = executionState.getExecutionDate();

        List<MetricLevelLtd> widgetDataList = new ArrayList<>(0);

        String metricName = dashboardConfiguration.getWidgetOneMetric();
        MetricLevelLtd metricLevelLtd = this.getMetricBalance(metricName, executionDate);
        metricLevelLtd.setMetricName(StringUtil.toTitleCase(metricLevelLtd.getMetricName()));
        widgetDataList.add(metricLevelLtd);

        metricName = dashboardConfiguration.getWidgetTwoMetric();
        metricLevelLtd = this.getMetricBalance(metricName, executionDate);
        metricLevelLtd.setMetricName(StringUtil.toTitleCase(metricLevelLtd.getMetricName()));
        widgetDataList.add(metricLevelLtd);

        metricName = dashboardConfiguration.getWidgetThreeMetric();
        metricLevelLtd = this.getMetricBalance(metricName, executionDate);
        metricLevelLtd.setMetricName(StringUtil.toTitleCase(metricLevelLtd.getMetricName()));
        widgetDataList.add(metricLevelLtd);

        metricName = dashboardConfiguration.getWidgetFourMetric();
        metricLevelLtd = this.getMetricBalance(metricName, executionDate);
        metricLevelLtd.setMetricName(StringUtil.toTitleCase(metricLevelLtd.getMetricName()));
        widgetDataList.add(metricLevelLtd);

        return widgetDataList;

    }

    private MetricLevelLtd getMetricBalance(String metricName, Integer postingDate) {
        MetricLevelLtd metricLevelLtd = getWidgetData(metricName, postingDate);

        if(metricLevelLtd == null){
            metricLevelLtd = this.getEmptyBalance();
        }

        return metricLevelLtd;
    }

    private MetricLevelLtd getEmptyBalance() {
        BaseLtd balance =  BaseLtd.builder().beginningBalance(BigDecimal.valueOf(0L))
                .endingBalance(BigDecimal.valueOf(0L))
                .activity(BigDecimal.valueOf(0L)).build();
        return MetricLevelLtd.builder().metricName("").accountingPeriodId(0).postingDate(0).balance(balance).build();
    }

    private MetricLevelLtd getWidgetData(String metricName, Integer postingDate){
        Query query = buildQuery(metricName, postingDate);
        return dataService.findOne(query, MetricLevelLtd.class);
    }

    public Records.TrendAnalysisRecord getTrendAnalysisData() {
        Settings settings = settingsService.fetch();
        DashboardConfiguration dashboardConfiguration = settings.getDashboardConfiguration();
        String metricName = dashboardConfiguration.getTrendAnalysisGraphMetric();

        if(metricName != null && !metricName.isEmpty()) {
            List<MetricLevelLtd> balances = this.getBalancesForTrendAnalysisData(metricName);

            List<String> accountingPeriods = new ArrayList<>(0);
            List<BigDecimal> endingBalances = new ArrayList<>(0);
            for(MetricLevelLtd ltd :  balances) {

                String strAccountingPeriodId = String.valueOf(ltd.getAccountingPeriodId());
                String year = strAccountingPeriodId.substring(0, 4);
                String periodNumber = strAccountingPeriodId.substring(4);
                String accountingPeriodStr = year + "-" + periodNumber;
                accountingPeriods.add(accountingPeriodStr);
                endingBalances.add(ltd.getBalance().getEndingBalance());
            }

            return RecordFactory.createTrendAnalysisRecord(StringUtil.toTitleCase(metricName), accountingPeriods.toArray(new String[0]), endingBalances.toArray(new BigDecimal[0]));
        }

        return null;
    }

    public Records.MonthOverMonthMetricActivityRecord getMonthOverMonthActivityData() {
        Settings settings = settingsService.fetch();
        DashboardConfiguration dashboardConfiguration = settings.getDashboardConfiguration();
        String[] metricNames = dashboardConfiguration.getActivityGraphMetrics();

        if(metricNames != null && metricNames.length>0) {
            List<Records.MonthOverMonthActivityRecord> balances = this.getBalancesForMonthOverMonthActivity(metricNames);

            List<String> accountingPeriods = new ArrayList<>(0);
            Map<String, Map<String, BigDecimal>> activityBalances = new HashMap<>(0);

            for(Records.MonthOverMonthActivityRecord ltd :  balances) {

                String strAccountingPeriodId = String.valueOf(ltd.accountingPeriodId());
                String year = strAccountingPeriodId.substring(0, 4);
                String periodNumber = strAccountingPeriodId.substring(4);
                String accountingPeriodStr = year + "-" + periodNumber;
                accountingPeriods.add(accountingPeriodStr);
                if(!activityBalances.containsKey(accountingPeriodStr)) {
                    activityBalances.put(accountingPeriodStr, new HashMap<>());
                }

                activityBalances.get(accountingPeriodStr).put(ltd.metricName(), ltd.activityAmount());
            }

            List<Map<String, Object>> monthOverMonthActivityList = new ArrayList<>(0);

            for (Map.Entry<String, Map<String, BigDecimal>> entry : activityBalances.entrySet()) {
                String accountingPeriodId = entry.getKey();
                Map<String, BigDecimal> categories = entry.getValue();
                Map<String, Object> monthOverMonthActivities = new HashMap<>(0);
                monthOverMonthActivities.put("accountingPeriodId", accountingPeriodId);
                // Now you can work with the inner map
                for (Map.Entry<String, BigDecimal> categoryEntry : categories.entrySet()) {
                    String metricName = categoryEntry.getKey();
                    BigDecimal balance = categoryEntry.getValue();
                    // Process each category/balance pair
                    monthOverMonthActivities.put(metricName, balance);
                }
                monthOverMonthActivityList.add(monthOverMonthActivities);
            }

            List<Map<String, String>> metricSeries = new ArrayList<>(0);

            for(String metricName : metricNames) {
                Map<String, String> metricSeriesMap = new HashMap<>(0);
                metricSeriesMap.put("dataKey", metricName);
                metricSeriesMap.put("label", StringUtil.toTitleCase(metricName));
                metricSeries.add(metricSeriesMap);
            }

            return RecordFactory.createMonthOverMonthMetricActivityRecord(metricSeries, monthOverMonthActivityList);

            // return RecordFactory.createTrendAnalysisRecord(metricName, accountingPeriods.toArray(new String[0]), endingBalances.toArray(new BigDecimal[0]));
        }

        return null;
    }


    public List<MetricLevelLtd> getBalancesForTrendAnalysisData(String metricName) {

       return getBalancesForAccountingPeriods(metricName, 12);
    }


    public List<Records.MonthOverMonthActivityRecord> getBalancesForMonthOverMonthActivity(String[] metricName) {

        return getActivitiesForAccountingPeriods(metricName, 5);
    }

    public List<MetricLevelLtd> getBalancesForAccountingPeriods(String metricName, int numberOfPeriods) {

        MatchOperation matchStage = Aggregation.match(
                Criteria.where("metricName").is(metricName)
        );

        Aggregation aggregation = Aggregation.newAggregation(
                // Step 1: Filter by metricName
                matchStage,

                // Step 2: Sort by accountingPeriodId and postingDate in descending order
                Aggregation.sort(Sort.Direction.DESC, "accountingPeriodId"),

                // Step 3: Group by accountingPeriodId and take first (i.e., max postingDate)
                Aggregation.group("accountingPeriodId")
                        .first("metricName").as("metricName")
                        .first("postingDate").as("postingDate")
                        .first("accountingPeriodId").as("accountingPeriodId")
                        .first("balance").as("balance"),

                // Step 4: Sort grouped results by accountingPeriodId descending
                Aggregation.sort(Sort.Direction.DESC, "accountingPeriodId"),

                // Step 5: Limit to last 12
                Aggregation.limit(numberOfPeriods)
        );


        AggregationResults<MetricLevelLtd> results = this.dataService.getMongoTemplate().aggregate(aggregation, "MetricLevelLtd", MetricLevelLtd.class);
        return results.getMappedResults();
    }

    public List<Records.MonthOverMonthActivityRecord> getActivitiesForAccountingPeriods(String[] metricNames, int numberOfPeriods) {
        // Step 1: Match by metricName
        MatchOperation matchStage = Aggregation.match(
                Criteria.where("metricName").in(Arrays.asList(metricNames))
        );

        // Step 2: Sort by accountingPeriodId descending
        SortOperation sortStage = Aggregation.sort(Sort.Direction.DESC, "accountingPeriodId", "metricName");

        // Step 3: Group by accountingPeriodId and metricName
        GroupOperation groupStage = Aggregation.group("accountingPeriodId", "metricName")
                .sum(ConvertOperators.ToDouble.toDouble("$balance.activity")).as("activityAmount");

        // Step 4: Sort grouped results by accountingPeriodId descending
        SortOperation sortGrouped = Aggregation.sort(Sort.Direction.DESC, "_id.accountingPeriodId");

        // Step 5: Limit to last N periods (this limits overall result size, not per metric)
        LimitOperation limitStage = Aggregation.limit(numberOfPeriods * metricNames.length);

        // Step 6: Project output structure
        ProjectionOperation projectStage = Aggregation.project()
                .and("_id.metricName").as("metricName")
                .and("_id.accountingPeriodId").as("accountingPeriodId")
                .and("activityAmount").as("activityAmount");

        Aggregation aggregation = Aggregation.newAggregation(
                matchStage,
                sortStage,
                groupStage,
                sortGrouped,
                limitStage,
                projectStage
        );

        AggregationResults<Records.MonthOverMonthActivityRecord> results = this.dataService.getMongoTemplate()
                .aggregate(aggregation, "MetricLevelLtd", Records.MonthOverMonthActivityRecord.class);

        return results.getMappedResults();
    }



    public List<Records.RankedMetricRecord> getTop5MetricsByMaxEndingBalance() {
        ExecutionState executionState = executionStateService.getExecutionState();

        Aggregation aggregation = Aggregation.newAggregation(
                // Step 0: Filter by accountingPeriodId
                Aggregation.match(Criteria.where("postingDate").is(executionState.getExecutionDate())),

                // Step 1: Group by metricName and pick max(endingBalance)
                Aggregation.group("metricName")
                        .max("balance.endingBalance").as("endingBalance"),

                // Step 2: Sort by max ending balance descending
                Aggregation.sort(Sort.Direction.DESC, "endingBalance"),

                // Step 3: Limit to top 5
                Aggregation.limit(5)
        );

        AggregationResults<Document> results = this.dataService.getMongoTemplate().aggregate(
                aggregation,
                "MetricLevelLtd", // <-- your collection name
                Document.class
        );

        List<Records.RankedMetricRecord> rankedList = new ArrayList<>();
        int rank = 1;
        for (Document doc : results.getMappedResults()) {
            String metricName = doc.getString("_id");
            BigDecimal endingBalance = new BigDecimal(doc.get("endingBalance").toString());
            rankedList.add(RecordFactory.createRankedMetricRecord(rank++, StringUtil.toTitleCase(metricName), endingBalance));
        }

        return rankedList;
    }


}
