package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.InstrumentReplayState;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

public class InstrumentReplayStateWriter implements ItemWriter<InstrumentReplayState> {

    private final TenantDataSourceProvider dataSourceProvider;
    private final MongoItemWriter<InstrumentReplayState> delegate;
    private final TenantContextHolder tenantContextHolder;

    public InstrumentReplayStateWriter(MongoItemWriter<InstrumentReplayState> delegate,
                                 TenantDataSourceProvider dataSourceProvider,
                                 TenantContextHolder tenantContextHolder) {
        this.delegate = delegate;
        this.dataSourceProvider = dataSourceProvider;
        this.tenantContextHolder = tenantContextHolder;
    }

    @Override
    public void write(Chunk<? extends InstrumentReplayState> instrumentReplayStates) throws Exception {
        String tenant = tenantContextHolder.getTenant();
        if (tenant != null && !tenant.isEmpty()) {
            MongoTemplate mongoTemplate = dataSourceProvider.getDataSource(tenant);
            delegate.setTemplate(mongoTemplate);
        }
        delegate.write(instrumentReplayStates);
    }

}
