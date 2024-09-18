package com.reserv.dataloader.service;

import com.reserv.dataloader.accounting.AccountingPeriodGenerator;
import com.reserv.dataloader.datasource.accounting.rule.FileUploadActivityType;
import com.reserv.dataloader.entity.AccountingPeriod;
import com.reserv.dataloader.entity.ActivityLog;
import com.reserv.dataloader.entity.Settings;
import com.reserv.dataloader.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import javax.management.DescriptorRead;
import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SettingsService {

    private final DataService<Settings> dataService;
    private final AccountingPeriodService accountingPeriodService;
    @Autowired
    public SettingsService(DataService<Settings> dataService, AccountingPeriodService accountingPeriodService) {
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
