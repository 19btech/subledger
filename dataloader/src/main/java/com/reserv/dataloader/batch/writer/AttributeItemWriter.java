package com.reserv.dataloader.batch.writer;

import com.reserv.dataloader.config.TenantContextHolder;
import com.reserv.dataloader.component.TenantDataSourceProvider;
import com.reserv.dataloader.entity.Attributes;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;


public class AttributeItemWriter implements ItemWriter<Attributes> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoItemWriter<Attributes> delegate;
    private final TenantContextHolder tenantContextHolder;

    public AttributeItemWriter(MongoItemWriter<Attributes> delegate,
                               TenantDataSourceProvider dataSourceProvider,
                               TenantContextHolder tenantContextHolder) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
    }

    @Override
    public void write(Chunk<? extends Attributes> attributes) throws Exception {
        String tenant = tenantContextHolder.getTenant();
        if (tenant != null && !tenant.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
            delegate.setTemplate(mongoTemplate);
        }
        delegate.write(attributes);
    }
}
