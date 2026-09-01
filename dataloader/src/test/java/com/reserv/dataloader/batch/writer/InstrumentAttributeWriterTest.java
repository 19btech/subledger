package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.repository.AttributesRepository;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.InstrumentAttributeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.data.MongoItemWriter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Covers the "only create a new version if a versionable attribute actually changed" rule in
 * {@link InstrumentAttributeWriter#setEndDate}: non-versionable attributes are updated in place,
 * versionable attributes with an unchanged value extend the existing open version instead of
 * opening a new one, and a genuine value change still goes through the original open/close/
 * reclass-message chain.
 */
@ExtendWith(MockitoExtension.class)
class InstrumentAttributeWriterTest {

    private static final String TENANT_ID = "TENANT_1";
    private static final String ATTRIBUTE_ID = "ATTR_1";
    private static final String INSTRUMENT_ID = "INSTR_1";

    @Mock
    private MongoItemWriter<InstrumentAttribute> delegate;
    @Mock
    private TenantDataSourceProvider dataSourceProvider;
    @Mock
    private MemcachedRepository memcachedRepository;
    @Mock
    private InstrumentAttributeService instrumentAttributeService;
    @Mock
    private AccountingPeriodService accountingPeriodService;
    @Mock
    private ExecutionStateService executionStateService;
    @Mock
    private AttributesRepository attributesRepository;

    private InstrumentAttributeWriter writer;

    @BeforeEach
    void setUp() {
        writer = new InstrumentAttributeWriter(delegate, dataSourceProvider, memcachedRepository,
                instrumentAttributeService, accountingPeriodService, executionStateService, attributesRepository);
        writer.tenantId = TENANT_ID;
    }

    private static InstrumentAttribute newAttribute(long versionId, Map<String, Object> values, int periodId, int postingDate) {
        return InstrumentAttribute.builder()
                .id("id-" + versionId)
                .attributeId(ATTRIBUTE_ID)
                .instrumentId(INSTRUMENT_ID)
                .versionId(versionId)
                .previousVersionId(0L)
                .periodId(periodId)
                .postingDate(postingDate)
                .intEffectiveDate(postingDate)
                .effectiveDate(new java.util.Date(1_700_000_000_000L + periodId))
                .attributes(new HashMap<>(values))
                .build();
    }

    private static Map<String, Object> valueOf(Object v) {
        Map<String, Object> m = new HashMap<>();
        m.put("VALUE", v);
        return m;
    }

    @Test
    void versionableAttribute_valueChanged_opensNewVersionAndClosesOld() throws Exception {
        writer.attributeVersionableMap.put(ATTRIBUTE_ID, true);

        InstrumentAttribute existingOpen = newAttribute(100L, valueOf(1), 1, 20240101);
        when(instrumentAttributeService.getOpenInstrumentAttributes(ATTRIBUTE_ID, INSTRUMENT_ID, TENANT_ID))
                .thenReturn(new java.util.ArrayList<>(List.of(existingOpen)));

        InstrumentAttribute incoming = newAttribute(200L, valueOf(2), 2, 20240201);

        CacheList<Records.InstrumentAttributeReclassMessageRecord> messages = new CacheList<>();
        Chunk<InstrumentAttribute> result = writer.setEndDate(1L, List.of(incoming), messages);

        List<InstrumentAttribute> written = toList(result);
        assertEquals(2, written.size(), "expected both the closed-out old version and the new version to be written");

        InstrumentAttribute closedOld = written.stream().filter(a -> a.getVersionId() == 100L).findFirst().orElseThrow();
        InstrumentAttribute newVersion = written.stream().filter(a -> a.getVersionId() == 200L).findFirst().orElseThrow();

        assertNotNull(closedOld.getEndDate(), "old version should be closed off");
        assertEquals(100L, newVersion.getPreviousVersionId(), "new version should chain back to the old one");
        assertEquals(1, messages.getList().size(), "a value change should produce a reclass message");
    }

    @Test
    void versionableAttribute_valueUnchanged_extendsExistingInsteadOfNewVersion() throws Exception {
        writer.attributeVersionableMap.put(ATTRIBUTE_ID, true);

        InstrumentAttribute existingOpen = newAttribute(100L, valueOf(1), 1, 20240101);
        when(instrumentAttributeService.getOpenInstrumentAttributes(ATTRIBUTE_ID, INSTRUMENT_ID, TENANT_ID))
                .thenReturn(new java.util.ArrayList<>(List.of(existingOpen)));

        // Same value ("VALUE" -> 1), later posting/period date.
        InstrumentAttribute incoming = newAttribute(200L, valueOf(1), 2, 20240201);

        CacheList<Records.InstrumentAttributeReclassMessageRecord> messages = new CacheList<>();
        Chunk<InstrumentAttribute> result = writer.setEndDate(1L, List.of(incoming), messages);

        List<InstrumentAttribute> written = toList(result);
        assertEquals(1, written.size(), "unchanged value should not open a second version");

        InstrumentAttribute survivor = written.get(0);
        assertEquals(100L, survivor.getVersionId(), "the original open version's identity must be preserved");
        assertNull(survivor.getEndDate(), "the extended version must remain open");
        assertEquals(20240201, survivor.getPostingDate(), "postingDate should be brought forward");
        assertEquals(2, survivor.getPeriodId(), "periodId should be brought forward");
        assertTrue(messages.getList() == null || messages.getList().isEmpty(), "no reclass message for an unchanged value");
    }

    @Test
    void nonVersionableAttribute_updatesExistingRecordInPlace() throws Exception {
        writer.attributeVersionableMap.put(ATTRIBUTE_ID, false);

        InstrumentAttribute existingOpen = newAttribute(100L, valueOf("A"), 1, 20240101);
        when(instrumentAttributeService.getOpenInstrumentAttributes(ATTRIBUTE_ID, INSTRUMENT_ID, TENANT_ID))
                .thenReturn(new java.util.ArrayList<>(List.of(existingOpen)));

        InstrumentAttribute incoming = newAttribute(200L, valueOf("B"), 2, 20240201);

        CacheList<Records.InstrumentAttributeReclassMessageRecord> messages = new CacheList<>();
        Chunk<InstrumentAttribute> result = writer.setEndDate(1L, List.of(incoming), messages);

        List<InstrumentAttribute> written = toList(result);
        assertEquals(1, written.size(), "non-versionable attributes must never accumulate more than one record");

        InstrumentAttribute updated = written.get(0);
        assertEquals(100L, updated.getVersionId(), "identity of the single record must be preserved");
        assertEquals("B", updated.getAttributes().get("VALUE"), "value should be overwritten with the latest upload");
        assertNull(updated.getEndDate(), "non-versionable records are never closed");
        assertTrue(messages.getList() == null || messages.getList().isEmpty(), "no reclass message for a non-versionable overwrite");

        verify(instrumentAttributeService, never()).save(any(InstrumentAttribute.class));
    }

    @Test
    void firstEverLoad_noExistingOpenVersion_writesNewRecordPlainly() throws Exception {
        writer.attributeVersionableMap.put(ATTRIBUTE_ID, true);

        when(instrumentAttributeService.getOpenInstrumentAttributes(ATTRIBUTE_ID, INSTRUMENT_ID, TENANT_ID))
                .thenReturn(Collections.emptyList());

        InstrumentAttribute incoming = newAttribute(200L, valueOf(1), 1, 20240101);

        CacheList<Records.InstrumentAttributeReclassMessageRecord> messages = new CacheList<>();
        Chunk<InstrumentAttribute> result = writer.setEndDate(1L, List.of(incoming), messages);

        List<InstrumentAttribute> written = toList(result);
        assertEquals(1, written.size());
        assertEquals(200L, written.get(0).getVersionId());
        assertNull(written.get(0).getEndDate());
    }

    private static List<InstrumentAttribute> toList(Chunk<InstrumentAttribute> chunk) {
        return chunk.getItems();
    }
}
