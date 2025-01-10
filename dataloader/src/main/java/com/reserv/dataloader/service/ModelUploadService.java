package com.reserv.dataloader.service;

import com.reserv.dataloader.exception.AccountingPeriodClosedException;
import com.reserv.dataloader.exception.MetricNotFoundException;
import com.reserv.dataloader.exception.TransactionNotFoundException;
import com.reserv.dataloader.service.aggregation.AggregationService;
import com.reserv.dataloader.utils.ExcelFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fyntrac.common.dto.record.Records.TransactionNameRecord;
import com.fyntrac.common.dto.record.Records.MetricNameRecord;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;

@Service
@Slf4j
public class ModelUploadService {


    private final ExcelFileService excelFileService;
    private final TransactionService transactionService;
    private final AggregationService aggregationService;
    private final AccountingPeriodService accountingPeriodService;

    private final static String TRANSACTION_SHEET_NAME="i_transaction";
    private final static String METRIC_SHEET_NAME="i_metric";
    private final static String EXECUTION_DATE_SHEET_NAME="i_executiondate";

    @Autowired
    public ModelUploadService(ExcelFileService excelFileService
                            , TransactionService transactionService
                            , AggregationService aggregationService
                            , AccountingPeriodService accountingPeriodService) {
        this.excelFileService = excelFileService;
        this.transactionService = transactionService;
        this.aggregationService = aggregationService;
        this.accountingPeriodService = accountingPeriodService;
    }

    public boolean validateModel(Workbook model) throws Exception {
        //convert multipart file into POI workbook
        //read sheet i_transaction
        //ignore header row
        //read first column of the sheet
        //get list of transactions
        //check if transactions are valid
        //if valid continue else fail validation and throw exception
        return (validateTransactions(model) && validateMetrics(model) && validateExecutionDate(model));
    }

    public boolean validateModel(MultipartFile model) throws Exception {
        Workbook workbook = ExcelFileUtil.convertMultipartFileToWorkbook(model);
        return this.validateModel(workbook);
    }

    public boolean validateTransactions(Workbook model) throws Exception {
        List<String> transactions = this.excelFileService.readExcelSheet(model, TRANSACTION_SHEET_NAME);
        Collection<TransactionNameRecord> transactionNames = transactionService.fetchTransactinNames();
        for(String transaction : transactions) {
            boolean exists = transactionNames.stream()
                    .anyMatch(record -> record.transactionName().equalsIgnoreCase(transaction));

            if(!exists) {
                throw new TransactionNotFoundException("Transaction[" + transaction + " not a valid transaction in sheet ["+ TRANSACTION_SHEET_NAME +"], please correct model first and upload again");
            }
        }

        return Boolean.TRUE;
    }

    public boolean validateMetrics(Workbook model) throws Exception {

        List<String> metrics = this.excelFileService.readExcelSheet(model, METRIC_SHEET_NAME);
        Collection<MetricNameRecord> metricNames =aggregationService.fetchMetricNames();

        for(String metric : metrics) {
            boolean exists = metricNames.stream()
                    .anyMatch(record -> record.metricName().equalsIgnoreCase(metric));

            if(!exists) {
                throw new MetricNotFoundException("Metric[" + metric + " not valid metric in sheet ["+ METRIC_SHEET_NAME +"], please correct model first and upload again");
            }
        }

        return Boolean.TRUE;
    }

    public boolean validateExecutionDate(Workbook model) throws Exception {
        List<String> executionDates = this.excelFileService.readExcelSheet(model, EXECUTION_DATE_SHEET_NAME);
        String date = executionDates.get(0);
        Date executionDate = DateUtil.parseDate(date, new SimpleDateFormat("dd-MMM-yyyy"));
        return validateExeutionDate(executionDate);
    }
    public boolean validateExeutionDate(Date executionDate) throws AccountingPeriodClosedException {
        int accountingPeriodId = DateUtil.getAccountingPeriodId(executionDate);
        AccountingPeriod accountingPeriod =  accountingPeriodService.getAccountingPeriod(accountingPeriodId);
        if(accountingPeriod.getStatus() == 1) {
            throw new AccountingPeriodClosedException("Acconting Period is closed [" + accountingPeriod.toString() + "], please correct model first and upload again");
        }
        return Boolean.TRUE;
    }
}
