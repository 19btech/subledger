package com.reserv.dataloader.accounting;

import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.utils.DateUtil;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;


@Slf4j
@NoArgsConstructor
public class AccountingPeriodGenerator {

    Set<AccountingPeriod> accountingPeriods = new HashSet<>(0);
    public Set<AccountingPeriod>  generate(Date startingDate) throws ParseException {
        int day = DateUtil.getDay(startingDate);
        int month = DateUtil.getMonthForDate(startingDate) + 1;
        int year = DateUtil.getYearForDate(startingDate);

        Date accountingPeriodStartDate = DateUtil.formatStringToDate(month + "-" + day + "-" + year, "MM-dd-yyyy");
        Date accountingPeriodEndtDate = DateUtil.formatStringToDate(month + "-" + day + "-" + "2060", "MM-dd-yyyy");
        Date date = DateUtil.formatStringToDate(month + "-" + day + "-" + year, "MM-dd-yyyy");
        while(DateUtil.isBetween(date, accountingPeriodStartDate, accountingPeriodEndtDate)){
            generatePeriod(date);
            date = DateUtil.incrementMonth(date,12);
        }
        return accountingPeriods;
    }

    private void generatePeriod(Date startingDate) throws ParseException {
        int day = DateUtil.getDay(startingDate);
        int month = DateUtil.getMonthForDate(startingDate) + 1;
        int year = DateUtil.getYearForDate(startingDate);

        Date date = DateUtil.formatStringToDate(month + "-" + day + "-" + year, "MM-dd-yyyy");
        Date fiscalPeriodEndingYear = DateUtil.addDays(date,364);
        int fiscalYear = DateUtil.getYearForDate(fiscalPeriodEndingYear);
        AccountingPeriod previousAccountingPeriod=null;
        for(int period=1; period<=12; period++) {
            AccountingPeriod accountingPeriod = generateAccountingPeriod(date, period, fiscalYear);
            if(previousAccountingPeriod == null){
                accountingPeriod.setPreviousAccountingPeriodId(0);
            }else{
                accountingPeriod.setPreviousAccountingPeriodId(previousAccountingPeriod.getPeriodId());
            }
            accountingPeriods.add(accountingPeriod);
            date = DateUtil.incrementMonth(date,1);
            previousAccountingPeriod =  accountingPeriod;
        }
    }

    public AccountingPeriod generateAccountingPeriod(Date periodDate, int period, int fiscalYear) throws ParseException {
        Date pDate =  DateUtil.convertDateToIST(periodDate);
        int month = DateUtil.getMonthForDate(pDate) + 1;
        int year = DateUtil.getYearForDate(pDate);
        String formattedMonth = String.format("%02d", period);
        Date nexMonthDate  = DateUtil.convertDateToIST(DateUtil.incrementMonth(pDate,1));
        return AccountingPeriod.builder()
                .startDate(pDate)
                .calendarMonth(month)
                .year(year)
                .days(DateUtil.compareDays(DateUtil.incrementMonth(pDate,1),pDate))
                .endDate(nexMonthDate)
                .fiscalPeriod(period)
                .periodId(Integer.parseInt(fiscalYear + formattedMonth))
                .period(fiscalYear + "-" + period).build();
    }

}
