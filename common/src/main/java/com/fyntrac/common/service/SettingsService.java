package com.fyntrac.common.service;

import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.DashboardConfiguration;
import com.fyntrac.common.entity.Settings;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.List;

@Service
@Slf4j
public class SettingsService {

    private final DataService<Settings> dataService;
    private final AccountingPeriodDataUploadService accountingPeriodService;
    @Autowired
    public SettingsService(DataService<Settings> dataService, AccountingPeriodDataUploadService accountingPeriodService) {
        this.dataService = dataService;
        this.accountingPeriodService = accountingPeriodService;
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
}
