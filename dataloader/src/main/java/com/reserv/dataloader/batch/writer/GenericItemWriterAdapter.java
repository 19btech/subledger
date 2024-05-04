package com.reserv.dataloader.batch.writer;

import com.reserv.dataloader.component.TenantContextHolder;
import com.reserv.dataloader.component.TenantDataSourceProvider;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

public class GenericItemWriterAdapter <T> implements ItemWriter<T> {

    private final MongoItemWriter<T> delegate;
    private final TenantDataSourceProvider dataSourceProvider;
    private final TenantContextHolder tenantContextHolder;

    public GenericItemWriterAdapter(MongoItemWriter<T> delegate,
                                  TenantDataSourceProvider dataSourceProvider,
                                  TenantContextHolder tenantContextHolder) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
    }

    @Override
    public void write(Chunk<? extends T> items) throws Exception {
        String tenant = tenantContextHolder.getTenant();
        if (tenant != null && !tenant.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
            delegate.setTemplate(mongoTemplate);
        }
        delegate.write(items);
    }
}
