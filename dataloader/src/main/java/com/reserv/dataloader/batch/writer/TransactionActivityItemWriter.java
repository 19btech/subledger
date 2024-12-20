package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.entity.TransactionActivityList;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.DateUtil;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;
import com.fyntrac.common.service.InstrumentAttributeService;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.service.AttributeService;
import com.fyntrac.common.service.AccountingPeriodService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TransactionActivityItemWriter implements ItemWriter<TransactionActivity> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoItemWriter<TransactionActivity> delegate;
    private final TenantContextHolder tenantContextHolder;
    private final MemcachedRepository memcachedRepository;
    private final InstrumentAttributeService instrumentAttributeService;
    private String tenantId;
    private String transactionActivityKey;
    private TransactionActivityList keyList;
    private AttributeService attributeService;
    private Collection<Attributes> attributes;
    private AccountingPeriodService accountingPeriodService;
    private long batchId;
    public TransactionActivityItemWriter(MongoItemWriter<TransactionActivity> delegate,
                                 TenantDataSourceProvider dataSourceProvider,
                                 TenantContextHolder tenantContextHolder
            , MemcachedRepository memcachedRepository
            , InstrumentAttributeService instrumentAttributeService
            , AttributeService attributeService
            , AccountingPeriodService accountingPeriodService) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
        this.memcachedRepository = memcachedRepository;
        this.instrumentAttributeService = instrumentAttributeService;
        this.attributeService = attributeService;
        this.accountingPeriodService = accountingPeriodService;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        JobParameters jobParameters = stepExecution.getJobParameters();
        // store the job parameters in a field

        this.transactionActivityKey = jobParameters.getString(com.fyntrac.common.utils.Key.aggregationKey());
        this.tenantId = jobParameters.getString("tenantId");
        this.batchId = jobParameters.getLong("batchId");

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
        attributes = attributeService.getReclassableAttributes();
    }

    @Override
    public void write(Chunk<? extends TransactionActivity> activity) throws Exception {
        String tenant = tenantContextHolder.getTenant();
        ReferenceData referenceData = this.memcachedRepository.getFromCache(this.tenantContextHolder.getTenant(), ReferenceData.class);
        AccountingPeriod currentAccountingPeriod = this.accountingPeriodService.getAccountingPeriod(referenceData.getCurrentAccountingPeriodId(), tenant);
        if (tenant != null && !tenant.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
            for(TransactionActivity transactionActivity:activity) {
                int accountingPeriodId = DateUtil.getAccountingPeriodId(transactionActivity.getTransactionDate());
                AccountingPeriod effectiveAccountingPeriod =  this.accountingPeriodService.getAccountingPeriod(accountingPeriodId, tenant);;

                if(effectiveAccountingPeriod != null){
                    if(effectiveAccountingPeriod.getStatus() !=0) {
                        transactionActivity.setAccountingPeriod(currentAccountingPeriod);
                    }else{
                        transactionActivity.setAccountingPeriod(effectiveAccountingPeriod);
                    }
                    transactionActivity.setOriginalPeriodId(effectiveAccountingPeriod.getPeriodId());
                }else {
                    transactionActivity.setAccountingPeriod(currentAccountingPeriod);
                    transactionActivity.setOriginalPeriodId(referenceData.getCurrentAccountingPeriodId());
                }
                this.setAttributes(transactionActivity);
                keyList.add(tenantContextHolder.getTenant() + "TA" + transactionActivity.hashCode());
                transactionActivity.setBatchId(batchId);
                this.memcachedRepository.putInCache(tenantContextHolder.getTenant() + "TA" + transactionActivity.hashCode(), transactionActivity);
            }
            delegate.setTemplate(mongoTemplate);
            delegate.write(activity);
            this.memcachedRepository.replaceInCache(this.transactionActivityKey, keyList, 43200);
        }
    }

    private void setAttributes(TransactionActivity transactionActivity) {
        InstrumentAttribute instrumentAttribute = this.getLatestInstrumentAttribute(transactionActivity);
        long instrumentAttributeVersionId = 0;
        if(instrumentAttribute != null) {
            instrumentAttributeVersionId = instrumentAttribute.getVersionId();
        }
        transactionActivity.setInstrumentAttributeVersionId(instrumentAttributeVersionId);

        Map<String, Object> attributes = this.getReclassableAttributes(instrumentAttribute.getAttributes());
            transactionActivity.setAttributes(attributes);

    }

    private InstrumentAttribute getLatestInstrumentAttribute(TransactionActivity transactionActivity) {
        return this.instrumentAttributeService.getInstrumentAttributeByPeriodId(this.tenantId
                , transactionActivity.getAttributeId()
                , transactionActivity.getInstrumentId()
                , transactionActivity.getPeriodId());
    }

    private Map<String, Object> getReclassableAttributes(Map<String, Object> instrumentAttributes) {
        Map<String, Object> reclassAttributes = new HashMap<>(0);
        for(Attributes attribute : attributes) {
            String attributeName = attribute.getAttributeName();
            Object obj = instrumentAttributes.get(attributeName);
            reclassAttributes.put(attributeName, obj);
        }
        return reclassAttributes;
    }
}
