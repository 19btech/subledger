package com.reserv.dataloader.service;

import com.reserv.dataloader.exception.AccountingPeriodClosedException;
import com.reserv.dataloader.exception.InstrumentAttributeNotFoundException;
import com.reserv.dataloader.exception.MetricNotFoundException;
import com.reserv.dataloader.exception.TransactionNotFoundException;
import com.reserv.dataloader.service.aggregation.AggregationService;
import com.reserv.dataloader.utils.ExcelFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fyntrac.common.dto.record.Records.TransactionNameRecord;
import com.fyntrac.common.dto.record.Records.MetricNameRecord;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.web.multipart.MultipartFile;
import com.fyntrac.common.service.TransactionService;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
    private final static List<String> instrumentAttributeTypes = new ArrayList<>(Arrays.asList("Current", "Previous", "First"));


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
        return (validateTransactions(model)
                && validateMetrics(model)
                && validateExecutionDate(model)
                && validateInstrumentAttributes(model)
                && excelFileService.validateInstrumentAttributeColumns(model)
                && excelFileService.validateTransactionActivityColumns(model)
                && excelFileService.validateMetricColumns(model));
    }

    public boolean validateModel(MultipartFile model) throws Exception {
        Workbook workbook = ExcelFileUtil.convertMultipartFileToWorkbook(model);
        return this.validateModel(workbook);
    }

    public boolean validateTransactions(Workbook model) throws Exception {
        List<String> transactions = this.excelFileService.readExcelSheet(model, ExcelFileService.TRANSACTION_SHEET_NAME);
        if(transactions == null || transactions.isEmpty()) {
            throw new TransactionNotFoundException("Transaction["+ ExcelFileService.TRANSACTION_SHEET_NAME +"] is empty, please correct model first and upload again");
        }
        Collection<TransactionNameRecord> transactionNames = transactionService.fetchTransactinNames();
        for(String transaction : transactions) {
            boolean exists = transactionNames.stream()
                    .anyMatch(record -> record.transactionName().equalsIgnoreCase(transaction));

            if(!exists) {
                throw new TransactionNotFoundException("Transaction[" + transaction + " not a valid transaction in sheet ["+ ExcelFileService.TRANSACTION_SHEET_NAME +"], please correct model first and upload again");
            }
        }

        return Boolean.TRUE;
    }

    public boolean validateInstrumentAttributes(Workbook model) throws Exception {
        List<String> iaTypes = this.excelFileService.readExcelSheet(model, ExcelFileService.INSTRUMENT_ATTRIBUTE_SHEET_NAME);
        if(iaTypes == null || iaTypes.isEmpty()) {
            throw new InstrumentAttributeNotFoundException("InstrumentAttribute["+ ExcelFileService.INSTRUMENT_ATTRIBUTE_SHEET_NAME +"] is empty, please correct model first and upload again");
        }
        for(String iatype : iaTypes) {
            boolean exists = instrumentAttributeTypes.stream()
                    .anyMatch(type -> type.equalsIgnoreCase(iatype));

            if(!exists) {
                throw new InstrumentAttributeNotFoundException("InstrumentAttribute[" + iatype + " not a valid type in sheet ["+ ExcelFileService.INSTRUMENT_ATTRIBUTE_SHEET_NAME +"], please correct model first and upload again");
            }
        }

        return Boolean.TRUE;
    }

    public boolean validateMetrics(Workbook model) throws Exception {

        try {
            List<String> metrics = this.excelFileService.readExcelSheet(model, ExcelFileService.METRIC_SHEET_NAME);
            if(metrics == null || metrics.isEmpty()) {
                throw new MetricNotFoundException("Metric[" + ExcelFileService.METRIC_SHEET_NAME + "] is empty, please correct model first and upload again");
            }
            Collection<MetricNameRecord> metricNames = aggregationService.fetchMetricNames();

            for (String metric : metrics) {
                boolean exists = metricNames.stream()
                        .anyMatch(record -> record.metricName().equalsIgnoreCase(metric));

                if (!exists) {
                    throw new MetricNotFoundException("Metric[" + metric + " not valid metric in sheet [" + ExcelFileService.METRIC_SHEET_NAME + "], please correct model first and upload again");
                }
            }
            return Boolean.TRUE;
        }catch (Exception ex){
            throw ex;
        }

    }

    public boolean validateExecutionDate(Workbook model) throws Exception {
        List<String> executionDates = this.excelFileService.readExcelSheet(model, ExcelFileService.EXECUTION_DATE_SHEET_NAME);
        if(executionDates == null || executionDates.isEmpty()) {
            return Boolean.TRUE;
        }
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

    public List<String> getExcelHeaders(Workbook workbook, String sheetName) throws IOException {
        List<String> headers = new ArrayList<>();

            Sheet sheet = this.excelFileService.getSheet(workbook, sheetName);
            Row headerRow = sheet.getRow(0); // The first row is the header

            for (Cell cell : headerRow) {
                headers.add(cell.getStringCellValue().trim());
            }
        return headers;
    }
}
