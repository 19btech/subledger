package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.component.InstrumentReplayQueue;
import com.fyntrac.common.component.InstrumentReplaySet;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.ReferenceData;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.InstrumentAttributeService;
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

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class InstrumentAttributeWriter implements ItemWriter<InstrumentAttribute> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoItemWriter<InstrumentAttribute> delegate;
    private final MemcachedRepository memcachedRepository;
    private final InstrumentAttributeService instrumentAttributeService;
    private CacheList<Records.InstrumentAttributeReclassMessageRecord> reclassMessageRecords;
    private String tenantId;
    private long runId;
    private AccountingPeriodService accountingPeriodService;
    private long batchId;
    private CacheList<Records.TransactionActivityReplayRecord> transactionActivityReplayRecordCacheList;
    private ExecutionStateService executionStateService;
    private final InstrumentReplaySet instrumentReplaySet;
    private final InstrumentReplayQueue instrumentReplayQueue;
    private ExecutionState executionState;
    private Long jobId;

    public InstrumentAttributeWriter(MongoItemWriter<InstrumentAttribute> delegate,
                                     TenantDataSourceProvider dataSourceProvider,
                                     MemcachedRepository memcachedRepository,
                                     InstrumentAttributeService instrumentAttributeService,
                                     AccountingPeriodService accountingPeriodService
            , ExecutionStateService executionStateService
            , InstrumentReplaySet instrumentReplaySet
            , InstrumentReplayQueue instrumentReplayQueue) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.memcachedRepository = memcachedRepository;
        this.instrumentAttributeService = instrumentAttributeService;
        this.reclassMessageRecords = new CacheList<Records.InstrumentAttributeReclassMessageRecord>();
        this.runId=0;
        this.accountingPeriodService = accountingPeriodService;
        this.executionStateService = executionStateService;
        this.instrumentReplaySet = instrumentReplaySet;
        this.instrumentReplayQueue = instrumentReplayQueue;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        JobParameters jobParameters = stepExecution.getJobParameters();
        // store the job parameters in a field

        this.runId = jobParameters.getLong("run.id");
        this.tenantId = jobParameters.getString("tenantId");
        this.instrumentAttributeService.setTenant(tenantId);
        this.batchId = jobParameters.getLong("batchId");
        this.jobId = jobParameters.getLong("jobId");
        executionState = this.executionStateService.getExecutionState();
    }
    @Override
    public void write(Chunk<? extends InstrumentAttribute> instrumentAttributes) throws Exception {
        String dataKey = Key.reclassMessageList(this.tenantId, this.runId);
        if(this.memcachedRepository.ifExists(dataKey)) {
            reclassMessageRecords = this.memcachedRepository.getFromCache(dataKey, CacheList.class);
        }else{
            this.reclassMessageRecords = new CacheList<Records.InstrumentAttributeReclassMessageRecord>();
        }

            String replayDataKey = Key.replayMessageList(this.tenantId, this.runId);
            if(this.memcachedRepository.ifExists(dataKey)) {
                this.transactionActivityReplayRecordCacheList = this.memcachedRepository.getFromCache(dataKey, CacheList.class);
            }else{
                this.transactionActivityReplayRecordCacheList = new CacheList<Records.TransactionActivityReplayRecord>();
            }


            ReferenceData referenceData = this.memcachedRepository.getFromCache(this.tenantId, com.fyntrac.common.config.ReferenceData.class);
            List<InstrumentAttribute> combinedAttributes = new ArrayList<>(instrumentAttributes.getItems());

            if (this.tenantId != null && !this.tenantId.isEmpty()) {

            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(this.tenantId);
            if(mongoTemplate == null) {
                return;
            }

            for(InstrumentAttribute instrumentAttribute : combinedAttributes) {
                int effectivePeriodId = com.fyntrac.common.utils.DateUtil.getAccountingPeriodId(instrumentAttribute.getEffectiveDate());
                AccountingPeriod effectiveAccountingPeriod = this.accountingPeriodService.getAccountingPeriod(effectivePeriodId, this.tenantId);
                instrumentAttribute.setEndDate(null);
                instrumentAttribute.setPreviousVersionId(0L);
                if(effectiveAccountingPeriod != null){
                    if(effectiveAccountingPeriod.getStatus() !=0) {
                        instrumentAttribute.setPeriodId(referenceData.getCurrentAccountingPeriodId());
                    }else{
                        instrumentAttribute.setPeriodId(effectiveAccountingPeriod.getPeriodId());
                    }
                }else {
                    instrumentAttribute.setPeriodId(referenceData.getCurrentAccountingPeriodId());
                }
                instrumentAttribute.setBatchId(batchId);
                this.instrumentAttributeService.addIntoCache(this.tenantId, instrumentAttribute);
                int intEffectiveDate = DateUtil.convertToIntYYYYMMDDFromJavaDate(instrumentAttribute.getEffectiveDate());

                if(executionState != null && executionState.getExecutionDate() > intEffectiveDate) {
                    Records.InstrumentReplayRecord replayRecord = RecordFactory.createInstrumentReplayRecord(instrumentAttribute.getInstrumentId(), instrumentAttribute.getPostingDate(), intEffectiveDate);
                    instrumentReplaySet.add(tenantId, this.jobId,replayRecord);
                    instrumentReplayQueue.add(tenantId, this.jobId,replayRecord);
                }
            }
            delegate.setTemplate(mongoTemplate);
        }
        Chunk<InstrumentAttribute> updatedChunk = this.setEndDate(batchId, combinedAttributes);
        this.memcachedRepository.putInCache(dataKey, reclassMessageRecords);
        delegate.write(updatedChunk);
        this.memcachedRepository.putInCache(replayDataKey, this.transactionActivityReplayRecordCacheList);
    }


    private Chunk<InstrumentAttribute> setEndDate(long batchId , List<InstrumentAttribute> attributesList) {

        // Step 1: Group by attributeId and instrumentId
        Map<String, List<InstrumentAttribute>> groupedAttributes = new HashMap<>();
        List<InstrumentAttribute> openVersion = new ArrayList<>(0);
        for (InstrumentAttribute attribute : attributesList) {
            String key = attribute.getAttributeId() + "_" + attribute.getInstrumentId();
            groupedAttributes
                    .computeIfAbsent(key, k -> new ArrayList<>())
                    .add(attribute);
        }

        // Step 2: Sort each group by versionId and process endDate
        for (List<InstrumentAttribute> subChunk : groupedAttributes.values()) {
            // Sort the sub-chunk by versionId
            List<InstrumentAttribute> sortedSubChunk = subChunk.stream()
                    .sorted(Comparator.comparingInt(InstrumentAttribute::getPeriodId))
                    .collect(Collectors.toList());

            // Step 3: Set endDate of each attribute to effectiveDate of the next attribute
            for (int i = 0; i < sortedSubChunk.size(); i++) {

                InstrumentAttribute currentAttribute = sortedSubChunk.get(i);

                if(sortedSubChunk.size() > i+1) {
                    InstrumentAttribute nextAttribute = sortedSubChunk.get(i + 1);
                    // Set endDate of the current attribute to effectiveDate of the next attribute
                    currentAttribute.setEndDate(DateUtil.convertToUtc(nextAttribute.getEffectiveDate()));
                    nextAttribute.setPreviousVersionId(currentAttribute.getVersionId());
                    this.addReclassMessage(batchId, currentAttribute, nextAttribute);
                }

                if(i== 0) {
                    List<InstrumentAttribute> openInstrumentAttributes = this.instrumentAttributeService.getOpenInstrumentAttributes(currentAttribute.getAttributeId(), currentAttribute.getInstrumentId());
                    for(InstrumentAttribute openInstrumentAttribute : openInstrumentAttributes) {
                        openInstrumentAttribute.setEndDate(DateUtil.convertToUtc(currentAttribute.getEffectiveDate()));
                        currentAttribute.setPreviousVersionId(openInstrumentAttribute.getVersionId());
                        openVersion.add(openInstrumentAttribute);
                        this.addReclassMessage(batchId, openInstrumentAttribute, currentAttribute);
                    }
                }
            }
        }

        // Step 4: Flatten the grouped attributes back to a list
        List<InstrumentAttribute> flattenedList = groupedAttributes.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());

        // Step 5: Create a Chunk from the flattened list
        // Assuming you have a Pageable object to create the Chunk
        flattenedList.addAll(openVersion);
        return new Chunk<>(flattenedList);
    }

    /**
     * Add reclass message into CacheList
     * @param openInstrumentAttribute
     * @param currentAttribute
     */
    private void addReclassMessage(long batchId, InstrumentAttribute openInstrumentAttribute, InstrumentAttribute currentAttribute) {
        Records.InstrumentAttributeRecord openInstrumentAtt = RecordFactory.createInstrumentAttributeRecord(openInstrumentAttribute);
        Records.InstrumentAttributeRecord currentInstrumentAtt = RecordFactory.createInstrumentAttributeRecord(currentAttribute);
        Records.InstrumentAttributeReclassMessageRecord reclassMessageRecord = RecordFactory.createInstrumentAttributeReclassMessageRecord(tenantId, batchId, openInstrumentAtt, currentInstrumentAtt);
        this.reclassMessageRecords.add(reclassMessageRecord);
    }
}

