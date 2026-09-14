package com.fyntrac.common.service;

import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.entity.DashboardConfiguration;
import com.fyntrac.common.entity.Settings;
import com.fyntrac.common.enums.CustomTableType;
import com.fyntrac.common.enums.Currency;
import com.fyntrac.common.repository.ActivityDataValidationLogRepository;
import com.fyntrac.common.repository.AttributeLevelBalanceRepository;
import com.fyntrac.common.repository.CustomTableDefinitionRepository;
import com.fyntrac.common.repository.ErrorsRepository;
import com.fyntrac.common.repository.GeneralLedgerEnteryRepository;
import com.fyntrac.common.repository.GeneralLedgerEnteryStageRepository;
import com.fyntrac.common.repository.InstrumentAttributeRepository;
import com.fyntrac.common.repository.InstrumentLevelLtdRepository;
import com.fyntrac.common.repository.MetricLevelLtdRepository;
import com.fyntrac.common.repository.TransactionActivityRepository;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class SettingsService {

    private final DataService<Settings> dataService;
    private final AccountingPeriodDataUploadService accountingPeriodService;
    private final TransactionActivityRepository transactionActivityRepository;
    private final InstrumentAttributeRepository instrumentAttributeRepository;
    private final AttributeLevelBalanceRepository attributeLevelBalanceRepository;
    private final InstrumentLevelLtdRepository instrumentLevelLtdRepository;
    private final MetricLevelLtdRepository metricLevelLtdRepository;
    private final GeneralLedgerEnteryRepository generalLedgerEnteryRepository;
    private final GeneralLedgerEnteryStageRepository generalLedgerEnteryStageRepository;
    private final ErrorsRepository errorsRepository;
    private final ActivityDataValidationLogRepository activityDataValidationLogRepository;
    private final CustomTableDefinitionRepository customTableDefinitionRepository;

    @Autowired
    public SettingsService(DataService<Settings> dataService, AccountingPeriodDataUploadService accountingPeriodService,
                           TransactionActivityRepository transactionActivityRepository,
                           InstrumentAttributeRepository instrumentAttributeRepository,
                           AttributeLevelBalanceRepository attributeLevelBalanceRepository,
                           InstrumentLevelLtdRepository instrumentLevelLtdRepository,
                           MetricLevelLtdRepository metricLevelLtdRepository,
                           GeneralLedgerEnteryRepository generalLedgerEnteryRepository,
                           GeneralLedgerEnteryStageRepository generalLedgerEnteryStageRepository,
                           ErrorsRepository errorsRepository,
                           ActivityDataValidationLogRepository activityDataValidationLogRepository,
                           CustomTableDefinitionRepository customTableDefinitionRepository) {
        this.dataService = dataService;
        this.accountingPeriodService = accountingPeriodService;
        this.transactionActivityRepository = transactionActivityRepository;
        this.instrumentAttributeRepository = instrumentAttributeRepository;
        this.attributeLevelBalanceRepository = attributeLevelBalanceRepository;
        this.instrumentLevelLtdRepository = instrumentLevelLtdRepository;
        this.metricLevelLtdRepository = metricLevelLtdRepository;
        this.generalLedgerEnteryRepository = generalLedgerEnteryRepository;
        this.generalLedgerEnteryStageRepository = generalLedgerEnteryStageRepository;
        this.errorsRepository = errorsRepository;
        this.activityDataValidationLogRepository = activityDataValidationLogRepository;
        this.customTableDefinitionRepository = customTableDefinitionRepository;
    }

    public Settings saveFiscalPriod(Date fiscalPeriod) throws ParseException {
        Settings s = fetch();
        if(s != null) {
            s.setFiscalPeriodStartDate(DateUtil.convertDateToIST(fiscalPeriod));
            dataService.save(s);
            return s;
        }else{
            s = new Settings();
            s.setFiscalPeriodStartDate(DateUtil.convertDateToIST(fiscalPeriod));
            dataService.save(s);
            return s;
        }
    }

    public Settings save(Settings settings) {
        return this.dataService.save(settings);
    }
    public Settings saveRestatementMode(int restatementMode) {
        Settings s = fetch();
        if(s != null) {
            s.setRestatementMode(restatementMode);
            dataService.save(s);
            return s;
        }else{
            s = new Settings();
            s.setRestatementMode(restatementMode);
            dataService.save(s);
            return s;
        }
    }

    public DashboardConfiguration saveDashboardConfiguration(DashboardConfiguration dc) {
        try {
            Settings settings = this.fetch();
            settings.setDashboardConfiguration(dc);
            this.dataService.save(settings);
            return settings.getDashboardConfiguration();
        } catch (Throwable t) {
            // You can replace this with a proper logger if available
            log.error("Failed to save DashboardConfiguration: " + StringUtil.getStackTrace(t));
            t.printStackTrace();

            // Optionally, rethrow or return a default/failure object
            throw new RuntimeException("Unable to save DashboardConfiguration", t);
        }
    }


    public Settings fetch() {
        List<Settings> settingsList = dataService.fetchAllData(Settings.class);
        if (settingsList != null && !settingsList.isEmpty()) {
            return settingsList.get(0);
        }
        return null;
    }

    public Settings fetch(String tenantId) {
        List<Settings> settingsList = dataService.fetchAllData(tenantId, Settings.class);
        if (settingsList != null && !settingsList.isEmpty()) {
            return settingsList.get(0);
        }
        return null;
    }

    public Collection<AccountingPeriod> generateAccountingPeriod(Settings s) throws ParseException {
        return accountingPeriodService.generateAccountingPeriod(s);
    }

    public void updateAccountingPeriodStatus(int status) {
        this.accountingPeriodService.updateAccountingPeriodStatus(status);
    }

    public void reopenAccountingPeriods(String accountingPeriod) {
        this.accountingPeriodService.reopenAccountingPeriods(accountingPeriod);
    }

    public Collection<String> getClosedAccountingPeriods() {
        return this.accountingPeriodService.getClosedAccountingPeriods();
    }

    public void refreshSchema() {
        dataService.truncateDatabase();
    }

    public Settings saveCurrency(String code) {
        Settings s = fetch();
        if(s != null && code !=null) {
            Currency currency = Currency.fromCode(code);
            s.setCurrency(currency);
            dataService.save(s);

        }
        return s;
    }

    /**
     * Permanently deletes every activity-data record posted on the given date: the raw uploaded
     * activity (TransactionActivity, InstrumentAttribute, plus any OPERATIONAL custom-table
     * data), and everything computed from it for that date (attribute/instrument/metric-level
     * balances, GL entries, and the errors/validation logs tied to that posting date).
     * <p>
     * This does NOT remove the corresponding ActivityLog "load" records — those live in the
     * dataloader module, not common, and are deleted by the caller (SettingsController).
     *
     * @param postingDate the posting date (YYYYMMDD) whose activity data should be purged.
     * @return the number of documents deleted, keyed by collection name, for logging/auditing.
     */
    public Map<String, Long> deleteActivityData(Integer postingDate) {
        if (postingDate == null) {
            throw new IllegalArgumentException("A posting date is required to delete activity data.");
        }

        Map<String, Long> deletedCounts = new LinkedHashMap<>();

        deletedCounts.put("TransactionActivity", transactionActivityRepository.deleteByPostingDate(postingDate));
        deletedCounts.put("InstrumentAttribute", instrumentAttributeRepository.deleteByPostingDate(postingDate));
        deletedCounts.put("AttributeLevelLtd", attributeLevelBalanceRepository.deleteByPostingDate(postingDate));
        deletedCounts.put("InstrumentLevelLtd", instrumentLevelLtdRepository.deleteByPostingDate(postingDate));
        deletedCounts.put("MetricLevelLtd", metricLevelLtdRepository.deleteByPostingDate(postingDate));
        deletedCounts.put("GeneralLedgerEntery", generalLedgerEnteryRepository.deleteByPostingDate(postingDate));
        deletedCounts.put("GeneralLedgerEnteryStage", generalLedgerEnteryStageRepository.deleteByPostingDate(postingDate));
        deletedCounts.put("ActivityDataValidationLog", activityDataValidationLogRepository.deleteByPostingDate(postingDate));

        try {
            deletedCounts.put("Errors", errorsRepository.deleteByPostingDate(DateUtil.convertIntDateToUtc(postingDate)));
        } catch (ParseException e) {
            log.error("Failed to parse posting date [{}] while deleting Errors records: {}", postingDate, e.getMessage());
        }

        deletedCounts.putAll(deleteOperationalCustomTableData(postingDate));

        long total = deletedCounts.values().stream().mapToLong(Long::longValue).sum();
        log.warn("Deleted {} activity-data record(s) for posting date [{}]: {}", total, postingDate, deletedCounts);
        return deletedCounts;
    }

    /**
     * Deletes rows keyed by postingDate from every OPERATIONAL custom table (dynamic collections
     * registered via CustomTableDefinition). REFERENCE tables are lookup data, not activity data,
     * and are left untouched. A custom table with no column named "POSTINGDATE" is skipped.
     */
    private Map<String, Long> deleteOperationalCustomTableData(Integer postingDate) {
        Map<String, Long> deletedCounts = new LinkedHashMap<>();
        List<CustomTableDefinition> operationalTables =
                customTableDefinitionRepository.findByTableTypeIgnoreCase(CustomTableType.OPERATIONAL);

        for (CustomTableDefinition tableDef : operationalTables) {
            String postingDateColumn = tableDef.getColumns().stream()
                    .map(col -> col.getColumnName())
                    .filter(name -> name != null && name.equalsIgnoreCase("POSTINGDATE"))
                    .findFirst()
                    .orElse(null);

            if (postingDateColumn == null) {
                continue; // This custom table isn't posting-date keyed - nothing to purge.
            }

            Query query = new Query(Criteria.where(postingDateColumn).is(postingDate));
            long deleted = dataService.getMongoTemplate()
                    .remove(query, tableDef.getTableName())
                    .getDeletedCount();
            deletedCounts.put(tableDef.getTableName(), deleted);
        }

        return deletedCounts;
    }
}
