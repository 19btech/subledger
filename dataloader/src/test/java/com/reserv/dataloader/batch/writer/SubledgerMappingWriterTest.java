package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.SubledgerMapping;
import com.fyntrac.common.enums.EntryType;
import com.fyntrac.common.enums.Sign;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.data.MongoItemWriter;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SubledgerMappingWriterTest {

    @Test
    @SuppressWarnings("unchecked")
    void write_AddsOppositeSignRowWithReversedEntryType() throws Exception {
        MongoItemWriter<SubledgerMapping> delegate = mock(MongoItemWriter.class);
        TenantContextHolder tenantContextHolder = mock(TenantContextHolder.class);
        SubledgerMappingWriter writer = new SubledgerMappingWriter(delegate, mock(TenantDataSourceProvider.class),
                tenantContextHolder);

        writer.write(new Chunk<>(List.of(
                mapping("PURCHASE_UPB", Sign.POSITIVE, EntryType.DEBIT, "PRINCIPAL"),
                mapping("PURCHASE_UPB", Sign.POSITIVE, EntryType.CREDIT, "CASH CLEARING"))));

        ArgumentCaptor<Chunk<SubledgerMapping>> written = ArgumentCaptor.forClass(Chunk.class);
        verify(delegate).write(written.capture());
        List<SubledgerMapping> rows = written.getValue().getItems();
        assertEquals(4, rows.size());
        assertRow(rows.get(0), Sign.POSITIVE, EntryType.DEBIT, "PRINCIPAL");
        assertRow(rows.get(1), Sign.NEGATIVE, EntryType.CREDIT, "PRINCIPAL");
        assertRow(rows.get(2), Sign.POSITIVE, EntryType.CREDIT, "CASH CLEARING");
        assertRow(rows.get(3), Sign.NEGATIVE, EntryType.DEBIT, "CASH CLEARING");
    }

    private static SubledgerMapping mapping(String tx, Sign sign, EntryType entryType, String subType) {
        return SubledgerMapping.builder().transactionName(tx).sign(sign).entryType(entryType)
                .accountSubType(subType).build();
    }

    private static void assertRow(SubledgerMapping row, Sign sign, EntryType entryType, String subType) {
        assertEquals(sign, row.getSign());
        assertEquals(entryType, row.getEntryType());
        assertEquals(subType, row.getAccountSubType());
    }
}
