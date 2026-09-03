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
 * Covers the "only create a new version if a versionable attribute field actually changed" rule
 * in {@link InstrumentAttributeWriter#setEndDate}: attribute-name fields flagged non-versionable
 * in {@code Attributes} are updated in place, versionable fields with an unchanged value extend
 * the existing open version instead of opening a new one, a genuine change to a versionable field
 * still goes through the original open/close/reclass-message chain, and a document mixing both
 * kinds of fields is decided solely by its versionable fields. Also covers the "one upload can
 * carry several attribute-field changes for the same postingDate/effectiveDate, and those all
 * fold into a single version" rule.
 */
@ExtendWith(MockitoExtension.class)
class InstrumentAttributeWriterTest {

    private static final String TENANT_ID = "TENANT_1";
    private static final String ATTRIBUTE_ID = "ATTR_1";
    private static final String INSTRUMENT_ID = "INSTR_1";
    // attributeVersionableMap is keyed by Attributes.attributeName — i.e. the key inside
    // InstrumentAttribute.attributes (see valueOf() below), NOT the document-level attributeId.
    private static final String VALUE_FIELD = "VALUE";

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
        writer.attributeVersionableMap.put(VALUE_FIELD, true);

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
        writer.attributeVersionableMap.put(VALUE_FIELD, true);

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
        assertEquals(20240101, survivor.getPostingDate(), "postingDate must not change when no new version is created");
        assertEquals(1, survivor.getPeriodId(), "periodId must not change when no new version is created");
        assertTrue(messages.getList() == null || messages.getList().isEmpty(), "no reclass message for an unchanged value");
    }

    @Test
    void nonVersionableAttribute_updatesExistingRecordInPlace() throws Exception {
        writer.attributeVersionableMap.put(VALUE_FIELD, false);

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
        writer.attributeVersionableMap.put(VALUE_FIELD, true);

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

    @Test
    void mixedFields_nonVersionableFieldChangeAlone_doesNotOpenNewVersion() throws Exception {
        // ORDER_DATE is versionable, NOTES is not — mirrors one InstrumentAttribute document
        // carrying several attribute-definition fields with different isVersionable settings.
        writer.attributeVersionableMap.put("ORDER_DATE", true);
        writer.attributeVersionableMap.put("NOTES", false);

        Map<String, Object> openValues = new HashMap<>();
        openValues.put("ORDER_DATE", "2024-01-01");
        openValues.put("NOTES", "old note");
        InstrumentAttribute existingOpen = newAttribute(100L, openValues, 1, 20240101);
        when(instrumentAttributeService.getOpenInstrumentAttributes(ATTRIBUTE_ID, INSTRUMENT_ID, TENANT_ID))
                .thenReturn(new java.util.ArrayList<>(List.of(existingOpen)));

        Map<String, Object> incomingValues = new HashMap<>();
        incomingValues.put("ORDER_DATE", "2024-01-01"); // unchanged (versionable)
        incomingValues.put("NOTES", "new note");        // changed (non-versionable)
        InstrumentAttribute incoming = newAttribute(200L, incomingValues, 2, 20240201);

        CacheList<Records.InstrumentAttributeReclassMessageRecord> messages = new CacheList<>();
        Chunk<InstrumentAttribute> result = writer.setEndDate(1L, List.of(incoming), messages);

        List<InstrumentAttribute> written = toList(result);
        assertEquals(1, written.size(), "a non-versionable field changing alone must not open a new version");

        InstrumentAttribute survivor = written.get(0);
        assertEquals(100L, survivor.getVersionId(), "the original open version's identity must be preserved");
        assertNull(survivor.getEndDate(), "the extended version must remain open");
        assertEquals(20240101, survivor.getPostingDate(), "postingDate must not change when no new version is created");
        assertEquals(1, survivor.getPeriodId(), "periodId must not change when no new version is created");
        assertTrue(messages.getList() == null || messages.getList().isEmpty(),
                "no reclass message when only a non-versionable field changed");
    }

    @Test
    void mixedFields_versionableFieldChange_opensNewVersionRegardlessOfNonVersionableField() throws Exception {
        writer.attributeVersionableMap.put("ORDER_DATE", true);
        writer.attributeVersionableMap.put("NOTES", false);

        Map<String, Object> openValues = new HashMap<>();
        openValues.put("ORDER_DATE", "2024-01-01");
        openValues.put("NOTES", "same note");
        InstrumentAttribute existingOpen = newAttribute(100L, openValues, 1, 20240101);
        when(instrumentAttributeService.getOpenInstrumentAttributes(ATTRIBUTE_ID, INSTRUMENT_ID, TENANT_ID))
                .thenReturn(new java.util.ArrayList<>(List.of(existingOpen)));

        Map<String, Object> incomingValues = new HashMap<>();
        incomingValues.put("ORDER_DATE", "2024-02-01"); // changed (versionable)
        incomingValues.put("NOTES", "same note");       // unchanged (non-versionable)
        InstrumentAttribute incoming = newAttribute(200L, incomingValues, 2, 20240201);

        CacheList<Records.InstrumentAttributeReclassMessageRecord> messages = new CacheList<>();
        Chunk<InstrumentAttribute> result = writer.setEndDate(1L, List.of(incoming), messages);

        List<InstrumentAttribute> written = toList(result);
        assertEquals(2, written.size(), "a versionable field changing must open a new version");

        InstrumentAttribute closedOld = written.stream().filter(a -> a.getVersionId() == 100L).findFirst().orElseThrow();
        InstrumentAttribute newVersion = written.stream().filter(a -> a.getVersionId() == 200L).findFirst().orElseThrow();
        assertNotNull(closedOld.getEndDate(), "old version should be closed off");
        assertEquals(100L, newVersion.getPreviousVersionId(), "new version should chain back to the old one");
        assertEquals(1, messages.getList().size(), "a versionable field change should produce a reclass message");
    }

    @Test
    void sameSubmissionDate_multipleAttributeRows_mergeIntoOneVersion() throws Exception {
        // ORDER_DATE and NOTES are both versionable, but arrive as two separate rows in the same
        // upload for the same postingDate/effectiveDate (e.g. one attribute field per line).
        writer.attributeVersionableMap.put("ORDER_DATE", true);
        writer.attributeVersionableMap.put("NOTES", true);

        when(instrumentAttributeService.getOpenInstrumentAttributes(ATTRIBUTE_ID, INSTRUMENT_ID, TENANT_ID))
                .thenReturn(Collections.emptyList());

        Map<String, Object> row1Values = new HashMap<>();
        row1Values.put("ORDER_DATE", "2024-01-01");
        InstrumentAttribute row1 = newAttribute(100L, row1Values, 1, 20240101);

        Map<String, Object> row2Values = new HashMap<>();
        row2Values.put("NOTES", "first note");
        // Same postingDate/periodId/effectiveDate as row1 -> same version.
        InstrumentAttribute row2 = InstrumentAttribute.builder()
                .id("id-101")
                .attributeId(ATTRIBUTE_ID)
                .instrumentId(INSTRUMENT_ID)
                .versionId(101L)
                .previousVersionId(0L)
                .periodId(1)
                .postingDate(20240101)
                .intEffectiveDate(20240101)
                .effectiveDate(row1.getEffectiveDate())
                .attributes(new HashMap<>(row2Values))
                .build();

        CacheList<Records.InstrumentAttributeReclassMessageRecord> messages = new CacheList<>();
        Chunk<InstrumentAttribute> result = writer.setEndDate(1L, List.of(row1, row2), messages);

        List<InstrumentAttribute> written = toList(result);
        assertEquals(1, written.size(), "rows sharing postingDate and effectiveDate must merge into a single version");

        InstrumentAttribute merged = written.get(0);
        assertEquals("2024-01-01", merged.getAttributes().get("ORDER_DATE"));
        assertEquals("first note", merged.getAttributes().get("NOTES"));
        assertTrue(messages.getList() == null || messages.getList().isEmpty());
    }

    @Test
    void differentDate_versionableFieldChange_stillOpensSeparateVersion() throws Exception {
        // Guards against over-merging: two rows for DIFFERENT postingDate/effectiveDate must NOT
        // be folded together, even though they belong to the same attributeId+instrumentId group.
        writer.attributeVersionableMap.put(VALUE_FIELD, true);

        when(instrumentAttributeService.getOpenInstrumentAttributes(ATTRIBUTE_ID, INSTRUMENT_ID, TENANT_ID))
                .thenReturn(Collections.emptyList());

        InstrumentAttribute row1 = newAttribute(100L, valueOf(1), 1, 20240101);
        InstrumentAttribute row2 = newAttribute(200L, valueOf(2), 2, 20240201);

        CacheList<Records.InstrumentAttributeReclassMessageRecord> messages = new CacheList<>();
        Chunk<InstrumentAttribute> result = writer.setEndDate(1L, List.of(row1, row2), messages);

        List<InstrumentAttribute> written = toList(result);
        assertEquals(2, written.size(), "different dates must still open a new version, not merge");
        assertEquals(1, messages.getList().size());
    }

    private static List<InstrumentAttribute> toList(Chunk<InstrumentAttribute> chunk) {
        return chunk.getItems();
    }
}
