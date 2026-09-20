package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.InstrumentLevelLtd;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.aggregation.InstrumentLevelAggregationService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InstrumentLevelLtdFlatteningWriterTest {

    @Mock private MongoItemWriter<InstrumentLevelLtd> delegate;
    @Mock private TenantDataSourceProvider dataSourceProvider;
    @Mock private TenantContextHolder tenantContextHolder;
    @Mock private MemcachedRepository memcachedRepository;
    @Mock private MongoTemplate mongoTemplate;
    @Mock private InstrumentLevelAggregationService aggregationService;
    @Mock private DataService<InstrumentLevelLtd> dataService;

    private InstrumentLevelLtdFlatteningWriter writer;

    @BeforeEach
    void setUp() {
        when(aggregationService.getDataService()).thenReturn(dataService);
        when(dataService.getMongoTemplate()).thenReturn(mongoTemplate);
        // No rows exist yet for this posting date, and no prior posting date to carry a balance from.
        when(mongoTemplate.find(any(), eq(InstrumentLevelLtd.class))).thenReturn(List.of());
        lenient().when(dataService.findOne(any(), eq(InstrumentLevelLtd.class))).thenReturn(null);
        // getTenant() is static on TenantContextHolder (the writer calls it through the instance)
        TenantContextHolder.setTenant("TNT");
        lenient().when(dataSourceProvider.getDataSource("TNT")).thenReturn(mongoTemplate);

        writer = new InstrumentLevelLtdFlatteningWriter(delegate, dataSourceProvider, tenantContextHolder,
                memcachedRepository, mongoTemplate, aggregationService, "TNT", 42L);
    }

    @AfterEach
    void tearDown() {
        TenantContextHolder.clear();
    }

    @Test
    void newRowGetsAnIdBeforeItIsFirstWritten() throws Exception {
        writer.write(chunk(record("INST-1", "10.00")));

        InstrumentLevelLtd written = single(captureWritten());
        assertNotNull(written.getId(), "id must be fixed before the first upsert, or every later write inserts a new document");
        assertEquals(new BigDecimal("10.0000"), written.getBalance().getActivity());
    }

    @Test
    void laterChunkRewritesTheSameDocumentAndOnlyTouchedRows() throws Exception {
        writer.write(chunk(record("INST-1", "10.00"), record("INST-2", "5.00")));
        List<InstrumentLevelLtd> first = captureWritten();
        assertEquals(2, first.size());
        String inst1Id = first.stream().filter(l -> l.getInstrumentId().equals("INST-1")).findFirst().orElseThrow().getId();

        reset(delegate);
        writer.write(chunk(record("INST-1", "2.50")));

        InstrumentLevelLtd rewritten = single(captureWritten());
        assertEquals("INST-1", rewritten.getInstrumentId(), "untouched INST-2 must not be re-written");
        assertEquals(inst1Id, rewritten.getId(), "same document must be replaced, not inserted again");
        assertEquals(new BigDecimal("12.5000"), rewritten.getBalance().getActivity());
        assertEquals(new BigDecimal("12.5000"), rewritten.getBalance().getEndingBalance());
    }

    private static Records.InstrumentLevelLtdRecord record(String instrumentId, String amount) {
        return new Records.InstrumentLevelLtdRecord("DEFERRED_REVENUE", instrumentId, 20250630, 202506, new BigDecimal(amount));
    }

    @SafeVarargs
    private static Chunk<List<Records.InstrumentLevelLtdRecord>> chunk(Records.InstrumentLevelLtdRecord... records) {
        return new Chunk<>(List.of(List.of(records)));
    }

    @SuppressWarnings("unchecked")
    private List<InstrumentLevelLtd> captureWritten() throws Exception {
        ArgumentCaptor<Chunk<? extends InstrumentLevelLtd>> captor = ArgumentCaptor.forClass(Chunk.class);
        verify(delegate).write(captor.capture());
        return (List<InstrumentLevelLtd>) captor.getValue().getItems();
    }

    private static <T> T single(List<T> items) {
        assertEquals(1, items.size(), "expected exactly one written row but got " + items);
        return items.get(0);
    }
}
