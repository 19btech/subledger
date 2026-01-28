package com.fyntrac.gl.service;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.entity.ReclassValues;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AttributeService;
import com.fyntrac.common.service.DataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Service class that processes general ledger reclassification messages received from a Pulsar topic.
 */
@Service
@Slf4j
public class GeneralLedgerReclassService extends BaseGeneralLedgerService {

    private final DataService dataService;
    private final MemcachedRepository memcachedRepository;
    private final AttributeService attributeService;
    private Collection<Attributes> reclassAttributes;
    private final DatasourceService datasourceService;

    @Value("${fyntrac.chunk.size}")
    private int chunkSize;

    @Value("${fyntrac.thread.pool.size}")
    private int threadPoolSize;

    private String tenantId;

    /**
     * Constructor for GeneralLedgerReclassService.
     *
     * @param dataService         Service for data persistence.
     * @param memcachedRepository Repository for caching data.
     * @param attributeService    Service for retrieving attributes.
     */
    @Autowired
    public GeneralLedgerReclassService(DataService dataService,
                                       MemcachedRepository memcachedRepository,
                                       AttributeService attributeService
                                        , DatasourceService datasourceService) {
        this.dataService = dataService;
        this.memcachedRepository = memcachedRepository;
        this.attributeService = attributeService;
        this.reclassAttributes = new ArrayList<>(0);
        this.datasourceService = datasourceService;
    }

    /**
     * Initializes the reclassifiable attributes for the specified tenant.
     *
     * @param executionContext The context containing execution details.
     */
    @Override
    protected void initialize(Map<String, Object> executionContext) {
        try {
            tenantId = (String) executionContext.get("tenantId");
            this.datasourceService.addDatasource(tenantId);
            reclassAttributes = this.attributeService.getReclassableAttributes(tenantId);
        } catch (Exception e) {
            log.error("Error initializing reclass attributes for tenant {}: {}", tenantId, e.getMessage(), e);
        }
    }

    /**
     * Processes the reclassification messages in chunks using a thread pool.
     *
     * @param executionContext The context containing execution details.
     */
    @Override
    protected void perform(Map<String, Object> executionContext) throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(this.threadPoolSize);
        try {
            String dataKey = (String) executionContext.get("dataKey");
            CacheList<Records.InstrumentAttributeReclassMessageRecord> reclassMessages = this.memcachedRepository.getFromCache(dataKey, CacheList.class);
            if (reclassMessages == null) {
                log.warn("No reclass messages found in cache for dataKey: {}", dataKey);
                return;
            }

            int totalChunks = reclassMessages.getTotalChunks(chunkSize);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < totalChunks; i++) {
                List<Records.InstrumentAttributeReclassMessageRecord> reclassMessagesChunk = reclassMessages.getChunk(chunkSize, i);
                futures.add(executor.submit(() -> process(reclassMessagesChunk)));
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Error processing reclassification chunk: {}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("Error performing reclassification process: {}", e.getMessage(), e);
        } finally {
            executor.shutdown();
            String dataKey = (String) executionContext.get("dataKey");
            this.memcachedRepository.delete(dataKey);
        }
    }

    /**
     * Processes a chunk of reclassification messages.
     *
     * @param chunk The list of reclassification messages to process.
     */
    private void process(List<Records.InstrumentAttributeReclassMessageRecord> chunk) {
        Set<ReclassValues> reclassList = new HashSet<>();
        try {
            for (Records.InstrumentAttributeReclassMessageRecord messageRecord : chunk) {
                ReclassValues reclassValues = compareAttributeValues(messageRecord.batchId(), messageRecord.previousInstrumentAttribute(), messageRecord.currentInstrumentAttribute());
                if (reclassValues != null) {
                    reclassList.add(reclassValues);
                }
            }

            if (!reclassList.isEmpty()) {
                this.dataService.saveAll(reclassList, tenantId, ReclassValues.class);
            }
        } catch (Exception e) {
            log.error("Error processing reclass messages: {}", e.getMessage(), e);
        }
    }

    /**
     * Compares the attribute values between two instrument records and determines if reclassification is required.
     *
     * @param previous The previous instrument attribute record.
     * @param current  The current instrument attribute record.
     * @return A ReclassValues object if reclassification is needed, otherwise null.
     */
    private ReclassValues compareAttributeValues(long batchId, Records.InstrumentAttributeRecord previous, Records.InstrumentAttributeRecord current) {
        try {
            Map<String, Object> previousAttributes = previous.attributes();
            Map<String, Object> currentAttributes = current.attributes();
            ReclassValues reclassValues = null;

            if(this.reclassAttributes.isEmpty()) {
                reclassAttributes = this.attributeService.getReclassableAttributes(tenantId);
            }

            for (Attributes attribute : this.reclassAttributes) {
                if (attribute.getIsReclassable() == 1) { // Assuming isReclassable is 1 for TRUE
                    String attributeName = attribute.getAttributeName();
                    Object value1 = previousAttributes.get(attributeName);
                    Object value2 = currentAttributes.get(attributeName);

                    if (checkReclass(value1, value2)) {
                        reclassValues = this.buildReclassValuesObject(attributeName, batchId, value1, value2, previous, current);
                        break;
                    }
                }
            }
            return reclassValues;
        } catch (Exception e) {
            log.error("Error comparing attribute values: {}", e.getMessage(), e);
            return null;
        }
    }

    private Map<String, Object> getReclassableAttributes(Records.InstrumentAttributeRecord instrumentAttribute) {
        Map<String, Object> reclassAttributes = new HashMap<>(0);
        for(Attributes attribute : this.reclassAttributes) {
            String attributeName = attribute.getAttributeName();
            Object obj = instrumentAttribute.attributes().get(attributeName);
            reclassAttributes.put(attributeName, obj);
        }
        return reclassAttributes;
    }
    /**
     * Builds a ReclassValues object to represent the reclassification details.
     *
     * @param attributeName The name of the attribute.
     * @param oldValue      The previous value of the attribute.
     * @param newValue      The new value of the attribute.
     * @param previous      The previous instrument record.
     * @param current       The current instrument record.
     * @return A new ReclassValues object.
     */
    private ReclassValues buildReclassValuesObject(String attributeName, long batchId, Object oldValue, Object newValue,
                                                   Records.InstrumentAttributeRecord previous, Records.InstrumentAttributeRecord current) {
        Map<String, Object> reclassAttributes = getReclassableAttributes(current);
        return ReclassValues.builder()
                .attributeId(previous.attributeId())
                .attributeName(attributeName)
                .batchId(batchId)
                .instrumentId(previous.instrumentId())
                .previousPeriodId(previous.periodId())
                .previousVersionId(previous.versionId())
                .currentVersionId(current.versionId())
                .currentPeriodId(current.periodId())
                .oldValue(oldValue)
                .newValue(newValue)
                .attributes(reclassAttributes)
                .build();
    }

    /**
     * Checks if reclassification is required by comparing two attribute values.
     *
     * @param value1 The previous attribute value.
     * @param value2 The current attribute value.
     * @return True if reclassification is required, otherwise false.
     */
    private boolean checkReclass(Object value1, Object value2) {
        return (value1 == null && value2 != null) ||
                (value1 != null && value2 == null) ||
                (value1 != null && !value1.equals(value2));
    }

    /**
     * Concludes the reclassification process by clearing relevant cache entries.
     *
     * @param executionContext The context containing execution details.
     * @throws ExecutionException   If an error occurs during execution.
     * @throws InterruptedException If the operation is interrupted.
     */
    @Override
    protected void conclude(Map<String, Object> executionContext) throws ExecutionException, InterruptedException {
        try {
            String dataKey = (String) executionContext.get("dataKey");
            this.memcachedRepository.delete(dataKey);
        } catch (Exception e) {
            log.error("Error concluding reclassification process: {}", e.getMessage(), e);
        }
    }
}
