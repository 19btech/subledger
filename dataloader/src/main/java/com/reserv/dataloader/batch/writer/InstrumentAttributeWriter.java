package com.reserv.dataloader.batch.writer;

import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.InstrumentAttribute;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.service.InstrumentAttributeService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class InstrumentAttributeWriter implements ItemWriter<InstrumentAttribute> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoItemWriter<InstrumentAttribute> delegate;
    private final TenantContextHolder tenantContextHolder;
    private final MemcachedRepository memcachedRepository;
    private final InstrumentAttributeService instrumentAttributeService;
    public InstrumentAttributeWriter(MongoItemWriter<InstrumentAttribute> delegate,
                                 TenantDataSourceProvider dataSourceProvider,
                                 TenantContextHolder tenantContextHolder,
                                  MemcachedRepository memcachedRepository,
                                     InstrumentAttributeService instrumentAttributeService) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
        this.memcachedRepository = memcachedRepository;
        this.instrumentAttributeService = instrumentAttributeService;
    }

    @Override
    public void write(Chunk<? extends InstrumentAttribute> instrumentAttributes) throws Exception {
        String tenant = tenantContextHolder.getTenant();
        Map<String, com.fyntrac.common.entity.AccountingPeriod> accountingPeriodMap = this.memcachedRepository.getFromCache(com.fyntrac.common.utils.Key.accountingPeriodKey(tenant), Map.class);
        com.fyntrac.common.config.ReferenceData referenceData = this.memcachedRepository.getFromCache(this.tenantContextHolder.getTenant(), com.fyntrac.common.config.ReferenceData.class);
        List<InstrumentAttribute> combinedAttributes = new ArrayList<>(instrumentAttributes.getItems());
        if (tenant != null && !tenant.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);

            for(InstrumentAttribute instrumentAttribute : instrumentAttributes) {
                String accountingPeriod = DateUtil.getAccountingPeriod(instrumentAttribute.getEffectiveDate());
                AccountingPeriod effectiveAccountingPeriod = accountingPeriodMap.get(accountingPeriod);

                if(effectiveAccountingPeriod != null){
                    if(effectiveAccountingPeriod.getStatus() !=0) {
                        instrumentAttribute.setPeriodId(referenceData.getCurrentAccountingPeriodId());
                    }else{
                        instrumentAttribute.setPeriodId(effectiveAccountingPeriod.getPeriodId());
                    }
                }else {
                    instrumentAttribute.setPeriodId(referenceData.getCurrentAccountingPeriodId());
                }

                List<InstrumentAttribute> lastInstrumentAttributeRec = this.instrumentAttributeService.getLastOpenInstrumentAttributes(instrumentAttribute.getAttributeId(), instrumentAttribute.getInstrumentId());
                if(lastInstrumentAttributeRec.size() > 1) {
                    log.error("More than one open records found with endate is null {}", lastInstrumentAttributeRec);
                }
                    for(InstrumentAttribute ia : lastInstrumentAttributeRec) {
                        ia.setEndDate(instrumentAttribute.getEffectiveDate());
                        instrumentAttribute.setOriginationDate(ia.getOriginationDate());
                        combinedAttributes.add(ia);
                    }
            }


            delegate.setTemplate(mongoTemplate);
        }
        Chunk<InstrumentAttribute> updatedChunk = new Chunk<>(combinedAttributes);
        delegate.write(updatedChunk);
    }
}

