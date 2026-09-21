package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.config.TenantContextHolder;
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

    public TransactionActivityItemWriter(MongoItemWriter<TransactionActivity> delegate,
                                 TenantDataSourceProvider dataSourceProvider,
                                 TenantContextHolder tenantContextHolder
            , MemcachedRepository memcachedRepository
            , InstrumentAttributeService instrumentAttributeService
            , AttributeService attributeService
            , AccountingPeriodService accountingPeriodService
            , TransactionService transactionService
    , ExecutionStateService executionStateService
    ) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
        this.memcachedRepository = memcachedRepository;
        this.instrumentAttributeService = instrumentAttributeService;
        this.attributeService = attributeService;
        this.accountingPeriodService = accountingPeriodService;
        this.transactionService = transactionService;
        this.executionStateService = executionStateService;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        JobParameters jobParameters = stepExecution.getJobParameters();
        // store the job parameters in a field

        this.tenantId = jobParameters.getString("tenantId");
        this.batchId = jobParameters.getLong("batchId");
        this.runId = jobParameters.getLong("run.id");
        this.transactionActivityKey = com.fyntrac.common.utils.Key.aggregationKey(tenantId, runId);
        executionState = this.executionStateService.getExecutionState();
        if(this.transactionActivityKey == null) {
            return;
        }
        this.instrumentAttributeService.getDataService().setTenantId(tenantId);
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

        // One query for the chunk's open InstrumentAttribute rows instead of one per activity.
        // Nothing is written to InstrumentAttribute between here and the per-row enrichment
        // below, so prefetching sees exactly the same state the per-row lookups did.
        Set<String> chunkInstrumentIds = new HashSet<>();
        for (TransactionActivity transactionActivity : activity) {
            chunkInstrumentIds.add(transactionActivity.getInstrumentId().toUpperCase());
        }
        Map<String, List<InstrumentAttribute>> openAttributesByKey =
                this.instrumentAttributeService.getOpenInstrumentAttributes(chunkInstrumentIds, this.tenantId);

        // Process each transaction activity
        for (TransactionActivity transactionActivity : activity) {
            int accountingPeriodId = DateUtil.getAccountingPeriodId(transactionActivity.getPostingDate());
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

            this.setAttributes(transactionActivity, openAttributesByKey);

            transactionActivity.setBatchId(batchId);
            Transactions transaction = this.transactionService.getTransaction(transactionActivity.getTransactionName().toUpperCase());
            transactionActivity.setIsReplayable(transaction.getIsReplayable());
            transactionActivity.setPeriodId(transactionActivity.getAccountingPeriod().getPeriodId());
            if(executionState != null && (transactionActivity.getEffectiveDate() < executionState.getExecutionDate())) {

                if(transactionActivity.getEffectiveDate() == null){
                    transactionActivity.setEffectiveDate(transactionActivity.getPostingDate());
                }
            }
        }

        delegate.setTemplate(mongoTemplate);

        // Create new Chunk with combined list and write
        delegate.write(activity);
    }


    private void setAttributes(TransactionActivity transactionActivity,
                               Map<String, List<InstrumentAttribute>> openAttributesByKey) {
        InstrumentAttribute instrumentAttribute = this.getLatestInstrumentAttribute(transactionActivity, openAttributesByKey);
        long instrumentAttributeVersionId = 0;
        if (instrumentAttribute != null) {
            instrumentAttributeVersionId = instrumentAttribute.getVersionId();
            transactionActivity.setInstrumentAttributeVersionId(instrumentAttributeVersionId);
            Map<String, Object> attrs = this.getReclassableAttributes(instrumentAttribute.getAttributes());
            transactionActivity.setAttributes(attrs);
        } else {
            log.warn("No InstrumentAttribute found for instrumentId={} attributeId={} — skipping attribute enrichment.",
                    transactionActivity.getInstrumentId(), transactionActivity.getAttributeId());
            transactionActivity.setInstrumentAttributeVersionId(0L);
            transactionActivity.setAttributes(new HashMap<>());
        }
    }

    private InstrumentAttribute getLatestInstrumentAttribute(TransactionActivity transactionActivity,
                                                             Map<String, List<InstrumentAttribute>> openAttributesByKey) {
        // Same "attributeId_instrumentId" key the bulk query groups by; upper-cased to match how
        // the per-pair query used to match stored IDs.
        List<InstrumentAttribute> results = openAttributesByKey.get(
                transactionActivity.getAttributeId().toUpperCase() + "_"
                        + transactionActivity.getInstrumentId().toUpperCase());
        return (results != null && !results.isEmpty()) ? results.getFirst() : null;
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
