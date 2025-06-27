package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.component.InstrumentReplayQueue;
import com.fyntrac.common.component.InstrumentReplaySet;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.component.TransactionActivityQueue;
import com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.*;
import com.fyntrac.common.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

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
    private AttributeService attributeService;
    private Collection<Attributes> attributes;
    private AccountingPeriodService accountingPeriodService;
    private long batchId;
    private Long runId;
    private final ExecutionStateService executionStateService;
    private ExecutionState executionState;
    private final InstrumentReplaySet instrumentReplaySet;
    private final TransactionActivityQueue transactionActivityQueue;
    private final InstrumentReplayQueue instrumentReplayQueue;
    private Long jobId;

    public TransactionActivityItemWriter(MongoItemWriter<TransactionActivity> delegate,
                                 TenantDataSourceProvider dataSourceProvider,
                                 TenantContextHolder tenantContextHolder
            , MemcachedRepository memcachedRepository
            , InstrumentAttributeService instrumentAttributeService
            , AttributeService attributeService
            , AccountingPeriodService accountingPeriodService
            , TransactionService transactionService
    , ExecutionStateService executionStateService
    , InstrumentReplaySet instrumentReplaySet
    , TransactionActivityQueue transactionActivityQueue
    , InstrumentReplayQueue instrumentReplayQueue) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
        this.memcachedRepository = memcachedRepository;
        this.instrumentAttributeService = instrumentAttributeService;
        this.attributeService = attributeService;
        this.accountingPeriodService = accountingPeriodService;
        this.transactionService = transactionService;
        this.executionStateService = executionStateService;
        this.instrumentReplaySet = instrumentReplaySet;
        this.transactionActivityQueue = transactionActivityQueue;
        this.instrumentReplayQueue = instrumentReplayQueue;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        JobParameters jobParameters = stepExecution.getJobParameters();
        // store the job parameters in a field

        this.tenantId = jobParameters.getString("tenantId");
        this.batchId = jobParameters.getLong("batchId");
        this.runId = jobParameters.getLong("run.id");
        this.jobId = jobParameters.getLong("jobId");
        this.transactionActivityKey = com.fyntrac.common.utils.Key.aggregationKey(tenantId, runId);
        executionState = this.executionStateService.getExecutionState();
        if(this.transactionActivityKey == null) {
            return;
        }
        attributes = attributeService.getReclassableAttributes();
    }

    @Override
    public void write(Chunk<? extends TransactionActivity> activity) throws Exception {

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

            transactionActivity.setBatchId(batchId);
            Transactions transaction = this.transactionService.getTransaction(transactionActivity.getTransactionName().toUpperCase());
            transactionActivity.setIsReplayable(transaction.getIsReplayable());
            if(executionState != null && (transactionActivity.getEffectiveDate() < executionState.getExecutionDate())) {
                // add into replay List
                if(transactionActivity.getIsReplayable() != 0) {
                    Records.InstrumentReplayRecord replayRecord = RecordFactory.createInstrumentReplayRecord(transactionActivity.getInstrumentId()
                            , transactionActivity.getPostingDate(), transactionActivity.getEffectiveDate());
                    instrumentReplaySet.add(tenantId, this.jobId, replayRecord);
                    instrumentReplayQueue.add(tenantId, this.jobId, replayRecord);
                }else{
                    transactionActivity.setEffectiveDate(transactionActivity.getPostingDate());
                    transactionActivity.setTransactionDate(DateUtil.convertIntDateToUtc(transactionActivity.getPostingDate()));
                }
            }
            this.transactionActivityQueue.add(tenantId, jobId, transactionActivity);
        }

        delegate.setTemplate(mongoTemplate);

        // Create new Chunk with combined list and write
        delegate.write(activity);
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
