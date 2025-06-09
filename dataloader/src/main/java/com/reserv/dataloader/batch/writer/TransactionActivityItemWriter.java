package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.*;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.ZoneId;
import java.util.*;

@Slf4j
public class TransactionActivityItemWriter implements ItemWriter<TransactionActivity> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoItemWriter<TransactionActivity> delegate;
    private final TenantContextHolder tenantContextHolder;
    private final MemcachedRepository memcachedRepository;
    private final InstrumentAttributeService instrumentAttributeService;
    private final TransactionService transactionService;
    private String tenantId;
    private String transactionActivityKey;
    private TransactionActivityList keyList;
    private AttributeService attributeService;
    private Collection<Attributes> attributes;
    private AccountingPeriodService accountingPeriodService;
    private final TransactionActivityReversalService transactionActivityReversalService;
    private long batchId;
    private Long runId;
    private CacheList<Records.TransactionActivityReplayRecord> transactionActivityReplayRecordCacheList;

    public TransactionActivityItemWriter(MongoItemWriter<TransactionActivity> delegate,
                                 TenantDataSourceProvider dataSourceProvider,
                                 TenantContextHolder tenantContextHolder
            , MemcachedRepository memcachedRepository
            , InstrumentAttributeService instrumentAttributeService
            , AttributeService attributeService
            , AccountingPeriodService accountingPeriodService
            , TransactionActivityReversalService transactionActivityReversalService
            , TransactionService transactionService) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
        this.memcachedRepository = memcachedRepository;
        this.instrumentAttributeService = instrumentAttributeService;
        this.attributeService = attributeService;
        this.accountingPeriodService = accountingPeriodService;
        this.transactionActivityReplayRecordCacheList = new CacheList<Records.TransactionActivityReplayRecord>();
        this.transactionActivityReversalService = transactionActivityReversalService;
        this.transactionService = transactionService;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        JobParameters jobParameters = stepExecution.getJobParameters();
        // store the job parameters in a field

        this.tenantId = jobParameters.getString("tenantId");
        this.batchId = jobParameters.getLong("batchId");
        this.runId = jobParameters.getLong("run.id");
        this.transactionActivityKey = com.fyntrac.common.utils.Key.aggregationKey(tenantId, runId);

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
        String dataKey = Key.replayMessageList(this.tenantId, this.runId);

        if (this.memcachedRepository.ifExists(dataKey)) {
            this.transactionActivityReplayRecordCacheList = this.memcachedRepository.getFromCache(dataKey, CacheList.class);
        } else {
            this.transactionActivityReplayRecordCacheList = new CacheList<Records.TransactionActivityReplayRecord>();
        }

        String tenant = tenantContextHolder.getTenant();
        if (tenant == null || tenant.isEmpty()) {
            throw new IllegalArgumentException("Tenant cannot be null or empty");
        }

        List<TransactionActivity> reversalActivityList = new ArrayList<>();
        ReferenceData referenceData = this.memcachedRepository.getFromCache(tenant, ReferenceData.class);
        AccountingPeriod currentAccountingPeriod =
                this.accountingPeriodService.getAccountingPeriod(referenceData.getCurrentAccountingPeriodId(), tenant);

        MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);

        // Process each transaction activity
        for (TransactionActivity transactionActivity : activity) {
            int accountingPeriodId = DateUtil.getAccountingPeriodId(transactionActivity.getTransactionDate());
            AccountingPeriod effectiveAccountingPeriod = this.accountingPeriodService.getAccountingPeriod(accountingPeriodId, tenant);

            if (effectiveAccountingPeriod != null) {
                if (effectiveAccountingPeriod.getStatus() != 0) {
                    transactionActivity.setAccountingPeriod(currentAccountingPeriod);
                } else {
                    transactionActivity.setAccountingPeriod(effectiveAccountingPeriod);
                }
                transactionActivity.setOriginalPeriodId(effectiveAccountingPeriod.getPeriodId());
            } else {
                transactionActivity.setAccountingPeriod(currentAccountingPeriod);
                transactionActivity.setOriginalPeriodId(referenceData.getCurrentAccountingPeriodId());
            }

            this.setAttributes(transactionActivity);

            String key = tenant + "TA" + transactionActivity.hashCode();
            keyList.add(key);
            transactionActivity.setBatchId(batchId);
            this.memcachedRepository.putInCache(key, transactionActivity);
            Transactions transaction = this.transactionService.getTransaction(transactionActivity.getTransactionName().toUpperCase());
            if(transaction.getIsReplayable() == 1) {
                Collection<TransactionActivity> reversalActivity = isReplayActivity(transactionActivity);
                if (reversalActivity != null) {
                    for (TransactionActivity revActivity : reversalActivity) {
                        String revKey = tenant + "TA" + revActivity.hashCode();
                        keyList.add(revKey);
                        this.memcachedRepository.putInCache(revKey, revActivity);
                    }
                    reversalActivityList.addAll(reversalActivity);
                }
            }
        }

        delegate.setTemplate(mongoTemplate);

        // Combine original activities and reversal activities into a new List
        List<TransactionActivity> combinedActivities = new ArrayList<>();

        // Assuming Chunk has a method to retrieve elements as List<TransactionActivity>
        if (activity instanceof Chunk) {
            // This cast may be unsafe but assuming you know Chunk implementation
            combinedActivities.addAll(activity.getItems()); // Replace getItems() with actual method
        } else {
            // Fallback - iterate and add manually
            for (TransactionActivity ta : activity) {
                combinedActivities.add(ta);
            }
        }

        combinedActivities.addAll(reversalActivityList);

        // Create new Chunk with combined list and write
        Chunk<TransactionActivity> combinedChunk = new Chunk<>(combinedActivities);
        delegate.write(combinedChunk);

        this.memcachedRepository.replaceInCache(this.transactionActivityKey, keyList, 43200);
        this.memcachedRepository.putInCache(dataKey, this.transactionActivityReplayRecordCacheList);
    }


    private Collection<TransactionActivity> isReplayActivity(TransactionActivity activity) {
        boolean isReplayActiity = this.instrumentAttributeService.existsReplay(activity.getInstrumentId(), activity.getAttributeId(),  activity.getEffectiveDate());
        if(isReplayActiity) {
            try {
                Records.TransactionActivityReplayRecord replayRecord = RecordFactory.createTransactionActivityReplayRecord(activity.getInstrumentId(), activity.getAttributeId(), activity.getEffectiveDate());
                this.transactionActivityReplayRecordCacheList.add(replayRecord);
                return this.transactionActivityReversalService.bookReversal(activity);
            }catch (IllegalStateException e){
                log.warn(e.getLocalizedMessage());
            }
        }
        return null;
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
