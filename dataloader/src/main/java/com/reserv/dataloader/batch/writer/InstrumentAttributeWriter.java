package com.reserv.dataloader.batch.writer;

import com.reserv.dataloader.component.TenantContextHolder;
import com.reserv.dataloader.component.TenantDataSourceProvider;
import com.reserv.dataloader.entity.InstrumentAttribute;
import com.reserv.dataloader.entity.Transactions;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

public class InstrumentAttributeWriter implements ItemWriter<InstrumentAttribute> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoItemWriter<InstrumentAttribute> delegate;
    private final TenantContextHolder tenantContextHolder;

    public InstrumentAttributeWriter(MongoItemWriter<InstrumentAttribute> delegate,
                                 TenantDataSourceProvider dataSourceProvider,
                                 TenantContextHolder tenantContextHolder) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
    }

    @Override
    public void write(Chunk<? extends InstrumentAttribute> instrumentAttributes) throws Exception {
        String tenant = tenantContextHolder.getTenant();
        if (tenant != null && !tenant.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
            delegate.setTemplate(mongoTemplate);
        }
        delegate.write(instrumentAttributes);
    }
}

