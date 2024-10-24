package com.reserv.dataloader.batch.writer;

import com.reserv.dataloader.config.TenantContextHolder;
import com.reserv.dataloader.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.Transactions;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

public class TransactionItemWriter implements ItemWriter<Transactions> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoItemWriter<Transactions> delegate;
    private final TenantContextHolder tenantContextHolder;

    public TransactionItemWriter(MongoItemWriter<Transactions> delegate,
                                 TenantDataSourceProvider dataSourceProvider,
                                 TenantContextHolder tenantContextHolder) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
    }

    @Override
    public void write(Chunk<? extends Transactions> transactions) throws Exception {
        String tenant = tenantContextHolder.getTenant();
        if (tenant != null && !tenant.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
            delegate.setTemplate(mongoTemplate);
        }
        delegate.write(transactions);
    }
}
