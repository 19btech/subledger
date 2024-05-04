package com.reserv.dataloader.accounting;

import com.reserv.dataloader.entity.AccountingPeriod;
import com.reserv.dataloader.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import java.time.LocalDate;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;


@Slf4j
public class AccountingPeriodGenerator {

    Set<AccountingPeriod> accountingPeriods = new HashSet<>(0);
    public Set<AccountingPeriod>  generate(LocalDate startingDate) {
        Date date = DateUtil.convertToDate(startingDate);
        int day = DateUtil.getDay(date);
        int month = DateUtil.getMonthForDate(date);

        Date accountingPeriodStartDate = DateUtil.formatStringToDate(day + "-" + month + "-" + "2000", "dd-mm-yyyy");
        Date accountingPeriodEndtDate = DateUtil.formatStringToDate(day + "-" + month + "-" + "2060", "dd-mm-yyyy");
        while(DateUtil.isBetween(date, accountingPeriodStartDate, accountingPeriodEndtDate)){
            generatePeriod(date);
            date = DateUtil.getNextDate(date,12);
        }
        return accountingPeriods;
    }

    private void generatePeriod(Date startingDate) {
        Date date = startingDate;
        Date fiscalPeriodEndingYear = DateUtil.getNextDate(date,12);
        int fiscalYear = DateUtil.getYearForDate(fiscalPeriodEndingYear);

        for(int period=1; period<=12; period++) {
            AccountingPeriod accountingPeriod = generateAccountingPeriod(date, period, fiscalYear);
            accountingPeriods.add(accountingPeriod);
            date = DateUtil.getNextDate(date,1);
        }
    }

    public AccountingPeriod generateAccountingPeriod(Date priodDate, int period, int fiscalYear) {
        LocalDate startDate = DateUtil.convertToLocalDate(priodDate);
        LocalDate nexMonthDate  = DateUtil.convertToLocalDate(DateUtil.getNextDate(DateUtil.convertToDate(startDate),1));
        return AccountingPeriod.builder()
                .startDate(startDate)
                .calendarMonth(startDate.getMonthValue())
                .year(startDate.getYear())
                .days(startDate.getDayOfMonth())
                .endDate(nexMonthDate)
                .fiscalPeriod(period)
                .period(fiscalYear + "-" + period).build();
    }

}
