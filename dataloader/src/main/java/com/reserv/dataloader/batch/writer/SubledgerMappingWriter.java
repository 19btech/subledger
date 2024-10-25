package com.reserv.dataloader.batch.writer;

import  com.fyntrac.common.component.TenantDataSourceProvider;
import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.enums.EntryType;
import  com.fyntrac.common.enums.Sign;
import com.fyntrac.common.entity.SubledgerMapping;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.List;

public class SubledgerMappingWriter implements ItemWriter<SubledgerMapping> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoItemWriter<SubledgerMapping> delegate;
    private final TenantContextHolder tenantContextHolder;

    /**
     * Constructor
     * @param delegate
     * @param dataSourceProvider
     * @param tenantContextHolder
     */
    public SubledgerMappingWriter(MongoItemWriter<SubledgerMapping> delegate,
                                  TenantDataSourceProvider dataSourceProvider,
                                  TenantContextHolder tenantContextHolder) {
        this.dataSourceProvider = dataSourceProvider;
        this.delegate = delegate;
        this.tenantContextHolder = tenantContextHolder;
    }
    @Override
    public void write(Chunk<? extends SubledgerMapping> chunk) throws Exception {
        String tenant = tenantContextHolder.getTenant();
        if (tenant != null && !tenant.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
            delegate.setTemplate(mongoTemplate);
        }

        List<SubledgerMapping> mappings = new ArrayList<>(0);
        for (SubledgerMapping mapping : chunk) {
            mappings.add(mapping);
            mappings.add(this.generateMapping(mapping));
        }
        Chunk<SubledgerMapping> newchunk = new Chunk<>(mappings);
        delegate.write(newchunk);
    }

    private SubledgerMapping generateMapping(SubledgerMapping mapping) {
        SubledgerMapping newMapping =  mapping.clone();
        if(newMapping.getEntryType() == EntryType.CREDIT) {
            newMapping.setEntryType(EntryType.DEBIT);
        }

        if(newMapping.getSign() == Sign.POSITIVE) {
            newMapping.setSign(Sign.NEGATIVE);
        }

        return newMapping;
    }
}
