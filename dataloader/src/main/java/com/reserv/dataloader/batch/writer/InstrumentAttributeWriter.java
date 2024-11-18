package com.reserv.dataloader.batch.writer;

import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.InstrumentAttribute;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.service.InstrumentAttributeService;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.utils.Key;
import com.fyntrac.common.cache.collection.CacheList;

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

    public InstrumentAttributeWriter(MongoItemWriter<InstrumentAttribute> delegate,
                                 TenantDataSourceProvider dataSourceProvider,
                                  MemcachedRepository memcachedRepository,
                                     InstrumentAttributeService instrumentAttributeService) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.memcachedRepository = memcachedRepository;
        this.instrumentAttributeService = instrumentAttributeService;
        this.reclassMessageRecords = new CacheList<Records.InstrumentAttributeReclassMessageRecord>();
        this.runId=0;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        JobParameters jobParameters = stepExecution.getJobParameters();
        // store the job parameters in a field

        this.runId = jobParameters.getLong("run.id");
        this.tenantId = jobParameters.getString("tenantId");
        this.instrumentAttributeService.setTenant(tenantId);

    }
        @Override
    public void write(Chunk<? extends InstrumentAttribute> instrumentAttributes) throws Exception {
        String dataKey = Key.reclassMessageList(this.tenantId, this.runId);
        if(this.memcachedRepository.ifExists(dataKey)) {
            reclassMessageRecords = this.memcachedRepository.getFromCache(dataKey, CacheList.class);
        }

        Map<String, com.fyntrac.common.entity.AccountingPeriod> accountingPeriodMap = this.memcachedRepository.getFromCache(com.fyntrac.common.utils.Key.accountingPeriodKey(this.tenantId), Map.class);
        com.fyntrac.common.config.ReferenceData referenceData = this.memcachedRepository.getFromCache(this.tenantId, com.fyntrac.common.config.ReferenceData.class);
        List<InstrumentAttribute> combinedAttributes = new ArrayList<>(instrumentAttributes.getItems());
        if (this.tenantId != null && !this.tenantId.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(this.tenantId);

            for(InstrumentAttribute instrumentAttribute : combinedAttributes) {
                String accountingPeriod = DateUtil.getAccountingPeriod(instrumentAttribute.getEffectiveDate());
                AccountingPeriod effectiveAccountingPeriod = accountingPeriodMap.get(accountingPeriod);
                instrumentAttribute.setEndDate(null);
                if(effectiveAccountingPeriod != null){
                    if(effectiveAccountingPeriod.getStatus() !=0) {
                        instrumentAttribute.setPeriodId(referenceData.getCurrentAccountingPeriodId());
                    }else{
                        instrumentAttribute.setPeriodId(effectiveAccountingPeriod.getPeriodId());
                    }
                }else {
                    instrumentAttribute.setPeriodId(referenceData.getCurrentAccountingPeriodId());
                }
                this.instrumentAttributeService.addIntoCache(this.tenantId, instrumentAttribute);
            }
            delegate.setTemplate(mongoTemplate);
        }
        Chunk<InstrumentAttribute> updatedChunk = this.setEndDate(combinedAttributes);
        this.memcachedRepository.putInCache(dataKey, reclassMessageRecords);
        delegate.write(updatedChunk);
    }

    private Chunk<InstrumentAttribute> setEndDate(List<InstrumentAttribute> attributesList) {

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
                    currentAttribute.setEndDate(nextAttribute.getEffectiveDate());
                    this.addReclassMessage(currentAttribute, nextAttribute);
                }

                if(i== 0) {
                    List<InstrumentAttribute> openInstrumentAttributes = this.instrumentAttributeService.getOpenInstrumentAttributes(currentAttribute.getAttributeId(), currentAttribute.getInstrumentId());
                    for(InstrumentAttribute openInstrumentAttribute : openInstrumentAttributes) {
                        openInstrumentAttribute.setEndDate(currentAttribute.getEffectiveDate());
                        openVersion.add(openInstrumentAttribute);
                        this.addReclassMessage(openInstrumentAttribute, currentAttribute);
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
    private void addReclassMessage(InstrumentAttribute openInstrumentAttribute, InstrumentAttribute currentAttribute) {
        Records.InstrumentAttributeRecord openInstrumentAtt = RecordFactory.createInstrumentAttributeRecord(openInstrumentAttribute);
        Records.InstrumentAttributeRecord currentInstrumentAtt = RecordFactory.createInstrumentAttributeRecord(currentAttribute);
        Records.InstrumentAttributeReclassMessageRecord reclassMessageRecord = RecordFactory.createInstrumentAttributeReclassMessageRecord(tenantId, openInstrumentAtt, currentInstrumentAtt);
        this.reclassMessageRecords.add(reclassMessageRecord);
    }
}

