package com.reserv.dataloader.batch.writer;

import  com.fyntrac.common.component.TenantDataSourceProvider;
import  com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.Aggregation;
import com.fyntrac.common.entity.Transactions;
import com.reserv.dataloader.repository.AggregationMemcachedRepository;
import com.fyntrac.common.repository.MemcachedRepository;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

public class AggregationItemWriter implements ItemWriter<Aggregation> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoItemWriter<Aggregation> delegate;
    private final TenantContextHolder tenantContextHolder;
    private final AggregationMemcachedRepository memcachedRepository;

    public AggregationItemWriter(MongoItemWriter<Aggregation> delegate,
                                 TenantDataSourceProvider dataSourceProvider,
                                 TenantContextHolder tenantContextHolder,
                                 AggregationMemcachedRepository memcachedRepository) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
        this.memcachedRepository = memcachedRepository;
    }

    @Override
    public void write(Chunk<? extends Aggregation> aggregations) throws Exception {
        String tenant = tenantContextHolder.getTenant();
        if (tenant != null && !tenant.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
            delegate.setTemplate(mongoTemplate);
        }
        delegate.write(aggregations);
        this.memcachedRepository.putItemInCache(tenantContextHolder.getTenant(), aggregations);
    }
}
