package com.reserv.dataloader.batch.writer;

import  com.fyntrac.common.component.TenantDataSourceProvider;
import  com.fyntrac.common.config.ReferenceData;
import  com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.entity.TransactionActivityList;
import com.reserv.dataloader.repository.MemcachedRepository;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.Key;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.Map;

public class TransactionActivityItemWriter implements ItemWriter<TransactionActivity> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoItemWriter<TransactionActivity> delegate;
    private final TenantContextHolder tenantContextHolder;
    private final MemcachedRepository memcachedRepository;
    private String transactionActivityKey;
    private TransactionActivityList keyList;
    public TransactionActivityItemWriter(MongoItemWriter<TransactionActivity> delegate,
                                 TenantDataSourceProvider dataSourceProvider,
                                 TenantContextHolder tenantContextHolder
            , MemcachedRepository memcachedRepository) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
        this.memcachedRepository = memcachedRepository;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        JobParameters jobParameters = stepExecution.getJobParameters();
        // store the job parameters in a field

        this.transactionActivityKey = jobParameters.getString("agg.key");
        if(this.transactionActivityKey == null) {
            return;
        }
        boolean isKeyExists = this.memcachedRepository.ifExists(this.transactionActivityKey);
        if(!isKeyExists) {
            keyList =  new TransactionActivityList();
            this.memcachedRepository.putInCache(this.transactionActivityKey, keyList);
        }else{
            keyList = this.memcachedRepository.getFromCache(this.transactionActivityKey, TransactionActivityList.class);
        }
    }

    @Override
    public void write(Chunk<? extends TransactionActivity> activity) throws Exception {
        String tenant = tenantContextHolder.getTenant();
        Map<String, AccountingPeriod> accountingPeriodMap = this.memcachedRepository.getFromCache(Key.accountingPeriodKey(tenant), Map.class);
        ReferenceData referenceData = this.memcachedRepository.getFromCache(this.tenantContextHolder.getTenant(), ReferenceData.class);
        if (tenant != null && !tenant.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
            for(TransactionActivity transactionActivity:activity) {
                String accountingPeriod = DateUtil.getAccountingPeriod(transactionActivity.getTransactionDate());
                AccountingPeriod effectiveAccountingPeriod =  accountingPeriodMap.get(accountingPeriod);

                if(effectiveAccountingPeriod != null){
                    if(effectiveAccountingPeriod.getStatus() !=0) {
                        transactionActivity.setPeriodId(referenceData.getCurrentAccountingPeriodId());
                    }else{
                        transactionActivity.setPeriodId(effectiveAccountingPeriod.getPeriodId());
                    }
                    transactionActivity.setOriginalPeriodId(effectiveAccountingPeriod.getPeriodId());
                }else {
                    transactionActivity.setPeriodId(referenceData.getCurrentAccountingPeriodId());
                    transactionActivity.setOriginalPeriodId(referenceData.getCurrentAccountingPeriodId());
                }
                keyList.add(tenantContextHolder.getTenant() + "TA" + transactionActivity.hashCode());
                this.memcachedRepository.putInCache(tenantContextHolder.getTenant() + "TA" + transactionActivity.hashCode(), transactionActivity);
            }
            delegate.setTemplate(mongoTemplate);
            delegate.write(activity);
            this.memcachedRepository.replaceInCache(this.transactionActivityKey, keyList, 43200);
        }
    }
}
