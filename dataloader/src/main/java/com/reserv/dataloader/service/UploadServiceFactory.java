package com.reserv.dataloader.service;

import com.reserv.dataloader.datasource.accounting.rule.AccountingRules;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public class UploadServiceFactory implements ApplicationContextAware {

    private static ApplicationContext applicationContext;

    private static TransactionsUploadService transactionsUploadService;
    private static AttributesUploadService attributesUploadService;
    private static AggregateUploadService aggregateUploadService;
    private static AccountTypeUploadService accountTypeUploadService;
    private static ChartOfAccountUploadService chartOfAccountUploadService;
    private static SubledgerMappingUploadService subledgerMappingUploadService;
    private static InstrumentAttributeUploadService instrumentAttributeUploadService;
    private static TransactionActivityUploadService transactionActivityUploadService;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        transactionsUploadService = applicationContext.getBean(TransactionsUploadService.class);
        attributesUploadService = applicationContext.getBean(AttributesUploadService.class);
        aggregateUploadService = applicationContext.getBean(AggregateUploadService.class);
        accountTypeUploadService = applicationContext.getBean(AccountTypeUploadService.class);
        chartOfAccountUploadService = applicationContext.getBean(ChartOfAccountUploadService.class);
        subledgerMappingUploadService = applicationContext.getBean(SubledgerMappingUploadService.class);
        instrumentAttributeUploadService = applicationContext.getBean(InstrumentAttributeUploadService.class);
        transactionActivityUploadService = applicationContext.getBean(TransactionActivityUploadService.class);
    }

    public static UploadService getFileUploader(AccountingRules rule) {
        switch (rule) {
            case TRANSACIONS:
                return transactionsUploadService;
            case ATTRIBUTES:
                return attributesUploadService;
            case AGGREGATION:
                return aggregateUploadService;
            // Implement cases for other enum values
            case ACCOUNTTYPE:
                return accountTypeUploadService;
            case CHARTOFACCOUNT:
                return chartOfAccountUploadService;
            case SUBLEDGERMAPPING:
                return subledgerMappingUploadService;
            case INSTRUMENTATTRIBUTE:
                return instrumentAttributeUploadService;
            case TRANSACTIONACTIVITY:
                return transactionActivityUploadService;
            default:
                throw new IllegalArgumentException("Unsupported file uploader for enum value: " + rule);
        }
    }
}